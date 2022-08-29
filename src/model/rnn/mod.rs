use anyhow::{anyhow, Result};
use ndarray::prelude::*;
use ndarray::{Axis, concatenate, stack, Array2};
use std::any::Any;
use std::convert::{TryFrom, TryInto};
use std::fs;
use std::io::Read;
use tch::{nn, nn::Module, nn::OptimizerConfig, Device, nn::RNNConfig, Tensor, nn::RNN, kind::Kind, nn::VarStore};


use super::stream;

const IMAGE_DIM: i64 = 784;
const HIDDEN_NODES: i64 = 128;
const LABELS: i64 = 10;

#[derive(PartialEq)]
pub enum RecurrentLayer {
    Lstm,
    Gru,
}

pub struct NetworkConfig {
   seq_len: usize,
   stride: usize,
   embedding_dim: i64,
   hidden_dim: i64,
   dropout: f64,
   layer: RecurrentLayer,
}

impl NetworkConfig {
    pub fn new(seq_len: usize, 
               stride: usize,
               embedding_dim: i64,
               hidden_dim: i64,
               dropout: f64,
               layer: RecurrentLayer) -> NetworkConfig {
        NetworkConfig{
            seq_len,
            stride,
            embedding_dim,
            hidden_dim,
            dropout,
            layer,
        }
    }
}

pub fn save(reconstructed_packets: &stream::ReconstructedPackets, path: &String) -> std::io::Result<()> {
    let init_filename = format!("{}/{}", path, reconstructed_packets.get_tcp_tuple()).to_string();
    fs::write(init_filename, reconstructed_packets.get_init_tcp_message())?;
    let resp_filename = format!("{}/{}", path, reconstructed_packets.get_rev_tcp_tuple()).to_string();
    fs::write(resp_filename, reconstructed_packets.get_resp_tcp_message())?;

    Ok(())
}

pub fn load_file(init_filename: String, resp_filename: String) -> std::io::Result<(Vec<u8>,Vec<u8>)> {
    let init_msg = fs::read(init_filename)?;

    let resp_msg = fs::read(resp_filename)?;

    Ok((init_msg, resp_msg))
}

pub fn preprocessing(message: Vec<u8>, seq_len: usize, stride: usize) -> Array2<i64> {
    let nrows = message.len() - stride * seq_len;
    let mut byte_sequences = Array2::<i64>::default((nrows, seq_len+1)); 
    let mut i = 0;
    while i < (message.len() - seq_len) {
        let j = i + seq_len + 1;
        for index in 0..(seq_len+1) {
            byte_sequences[[i, index]] = i64::try_from(message[i+index]).unwrap();
        }
        i+=stride;
    }

    //println!("{:?}", byte_sequences);
    byte_sequences
}

// fn init_recurrent_layer<T: nn::RNN> (&vs, network_config: NetworkConfig, rnn_config: RNNConfig)-> T {
//     if (network_config.layer == RecurrentLayer::Lstm) {
//         nn::lstm(
//             &vs.root(),
//             network_config.embedding_dim,
//             network_config.hidden_dim,
//             rnn_config
//         )
//     }
//     else {
//         nn::gru(
//             &vs.root(),
//             network_config.embedding_dim,
//             network_config.hidden_dim,
//             rnn_config
//         )
//     }
// 
pub trait RecurrentModel {
    fn train_model_with_packet(&self, reconstructed_packets: &stream::ReconstructedPackets, direction: stream::PacketDirection) -> Result<()>;
    fn detect_conn(&self, reconstructed_packets: &stream::ReconstructedPackets, direction: stream::PacketDirection, threshold: f64) -> Result<bool>;
}

pub struct LSTMModel {
    rnn_config: RNNConfig,
    network_config: NetworkConfig,
    vs: VarStore,
    embedding_layer: nn::Embedding,
    recurrent_layers: nn::LSTM,
    dense_layer: nn::Linear
}


impl RecurrentModel for LSTMModel{
    fn train_model_with_packet(&self, reconstructed_packets: &stream::ReconstructedPackets, direction: stream::PacketDirection) -> Result<()>{
        match(direction) {
            stream::PacketDirection::Init => if reconstructed_packets.get_init_tcp_message().len() <= self.network_config.seq_len * self.network_config.stride {
                return Err(anyhow!("Message not long enough"));
            },
            stream::PacketDirection::Resp => if reconstructed_packets.get_resp_tcp_message().len() <= self.network_config.seq_len * self.network_config.stride {
                return Err(anyhow!("Message not long enough"));
            },
            stream::PacketDirection::Both => return Err(anyhow!("Direction not supported")),
        };

        let device = Device::Cuda(0);
        let byte_sequences = match(direction) {
            stream::PacketDirection::Init => preprocessing(reconstructed_packets.get_init_tcp_message(), self.network_config.seq_len, self.network_config.stride),
            stream::PacketDirection::Resp => preprocessing(reconstructed_packets.get_resp_tcp_message(), self.network_config.seq_len, self.network_config.stride),
            stream::PacketDirection::Both => panic!("PacketDirection::Both is not implemented yet")
        };

        let mut opt = nn::Adam::default().build(&self.vs, 1e-3)?;
        let nd_x = byte_sequences.slice(s![.., ..-1]).to_owned();
        let nd_y = byte_sequences.slice(s![.., -1]).to_owned();
        let x = Tensor::try_from(nd_x.clone()).unwrap();
        let y = Tensor::try_from(nd_y.clone()).unwrap();

        let embed_out = self.embedding_layer.forward(&x.to(device));
        //println!("{:?}", embed_out.size());
        let (rnn_out, _) = self.recurrent_layers.seq(&embed_out);
        //println!("{:?}", rnn_out.size());
        //TODO: FIX THIS
        let logits = self.dense_layer.forward(&rnn_out)
            .narrow(1, (self.network_config.seq_len-1).try_into().unwrap(), 1)
            .reshape(&[x.size()[0],256]);
        //println!("{:?}", logits.size());
        //println!("{:?}", Vec::<f64>::from(&logits));
        let loss = logits.cross_entropy_for_logits(&y.to(device));
        opt.backward_step(&loss);
        //let test_accuracy = net
        //    .forward(&m.test_images.to(device))
        //    .accuracy_for_logits(&m.test_labels.to(device));
        println!(
            "train loss: {:8.5}",
            f64::from(&loss),
        );

        Ok(())
    }

    fn detect_conn(&self, reconstructed_packets: &stream::ReconstructedPackets, direction: stream::PacketDirection, threshold: f64) -> Result<bool>{
        match(direction) {
            stream::PacketDirection::Init => if reconstructed_packets.get_init_tcp_message().len() <= self.network_config.seq_len * self.network_config.stride {
                return Err(anyhow!("Message not long enough"));
            },
            stream::PacketDirection::Resp => if reconstructed_packets.get_resp_tcp_message().len() <= self.network_config.seq_len * self.network_config.stride {
                return Err(anyhow!("Message not long enough"));
            },
            stream::PacketDirection::Both => return Err(anyhow!("Direction not supported")),
        };

        let device = Device::Cuda(0);
        let byte_sequences = match(direction) {
            stream::PacketDirection::Init => preprocessing(reconstructed_packets.get_init_tcp_message(), self.network_config.seq_len, self.network_config.stride),
            stream::PacketDirection::Resp => preprocessing(reconstructed_packets.get_resp_tcp_message(), self.network_config.seq_len, self.network_config.stride),
            stream::PacketDirection::Both => panic!("PacketDirection::Both is not implemented yet")
        };

        let nd_x = byte_sequences.slice(s![.., ..-1]).to_owned();
        let nd_y = byte_sequences.slice(s![.., -1]).to_owned();
        let x = Tensor::try_from(nd_x.clone()).unwrap();
        let y = Tensor::try_from(nd_y.clone()).unwrap();

        let embed_out = self.embedding_layer.forward(&x.to(device));
        //println!("{:?}", embed_out.size());
        let (rnn_out, _) = self.recurrent_layers.seq(&embed_out);
        //println!("{:?}", rnn_out.size());
        //TODO: FIX THIS
        let y_pred = self.dense_layer.forward(&rnn_out)
            .narrow(1, (self.network_config.seq_len-1).try_into().unwrap(), 1)
            .reshape(&[x.size()[0],256])
            .softmax(-1, Kind::Float)
            .multinomial(1, false)
            .reshape(&[x.size()[0]])
            .to(Device::Cpu);
        //println!("{:?}", y_pred.size());
        //println!("{:?}", Vec::<f64>::from(&y_pred));
        //let test_accuracy = net
        //    .forward(&m.test_images.to(device))
        //    .accuracy_for_logits(&m.test_labels.to(device));

        let arr_y_pred: ndarray::ArrayD<i64> = (&y_pred).try_into().unwrap();
        let mut num_differs: f64 = 0.0;

        for i in 0 .. nd_y.shape()[0] {
            if nd_y[i] != arr_y_pred[i] {
                //println!("{:?} {:?}", nd_y[i], arr_y_pred[i]);
                num_differs += 1.0;
            }
        }

        let message_len: f64 = (x.size()[0]) as f64 + (self.network_config.stride * self.network_config.seq_len) as f64;
        let anomaly_score = num_differs / message_len;
        println!("{:?}", anomaly_score);

        if num_differs > threshold {
            println!("Anomaly found");
            return Ok(false);
        }
        else {
            return Ok(true);
        }
    }

}

impl LSTMModel {
    pub fn new (network_config: NetworkConfig, rnn_config: RNNConfig) -> LSTMModel {
        let device = Device::Cuda(0);
        println!("Is cuda: {}", device.is_cuda());
        let vs = nn::VarStore::new(device);

        let embedding_layer = nn::embedding(
            &vs.root(),
            256,
            network_config.embedding_dim,
            Default::default(),
        );
        let dense_layer = nn::linear(&vs.root(), network_config.hidden_dim, 256, Default::default());
        
        let recurrent_layers = nn::lstm(
            &vs.root(),
            network_config.embedding_dim,
            network_config.hidden_dim,
            rnn_config
        );
        
        LSTMModel {
            network_config,
            rnn_config,
            vs,
            embedding_layer,
            recurrent_layers,
            dense_layer
        }       
    }
    

}
//pub fn training() -> model/none {
//}

//pub fn detection() {
//}

// pub fn run(config: NetworkConfig, rnn_config: RNNConfig) -> Result<()> {
//     let vs = nn::VarStore::new(Device::Cpu);
//     let mut opt = nn::Adam::default().build(&vs, 1e-3)?;
//     for epoch in 1..10 {
//         let embed_out = embedding_layer.forward(&x.to(device));
//         println!("{:?}", embed_out.size());
//         let (rnn_out, _) = rnn_layer.seq(&embed_out);
//         println!("{:?}", rnn_out.size());
//         let logits = dense_layer.forward(&rnn_out)
//             .narrow(1, 4, 1)
//             .reshape(&[1,256]);
//         println!("{:?}", logits.size());
//         //println!("{:?}", Vec::<f64>::from(&logits));
//         let loss = logits.cross_entropy_for_logits(&y.to(device));
//         opt.backward_step(&loss);
//         //let test_accuracy = net
//         //    .forward(&m.test_images.to(device))
//         //    .accuracy_for_logits(&m.test_labels.to(device));
//         println!(
//             "epoch: {:4} train loss: {:8.5}",
//             epoch,
//             f64::from(&loss),
//         );
//     }
//     Ok(())
// }
