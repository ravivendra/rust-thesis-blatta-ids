mod main

use blatta_stream::stream;
use smoltcp::phy::wait as phy_wait;
use smoltcp::phy::{Device, RawSocket, RxToken};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetFrame, PrettyPrinter, Ipv4Packet, EthernetProtocol, IpProtocol, TcpPacket};
use std::env;
use std::os::unix::io::AsRawFd;
use std::str;
use std::sync::mpsc;
use std::thread;

use tch::nn::RNNConfig;

//use pcap_file::pcap::{PcapParser, PcapReader};
//use pcap_file::PcapError;
//use std::fs::File;

//mod stream;
mod rnn;

use rnn::RecurrentModel;

fn main() {
    let ifname = env::args().nth(1).unwrap();
    let num_training_conn = 1;
    let path = "/home/baskoro/Documents/Research/IDS/data/".to_string();
    //let mut socket = RawSocket::new(ifname.as_ref()).unwrap();
    let port_filter = Vec::from([80u16]);
    let mut srt_controller = stream::StreamReaderController::new(port_filter, false, ifname);

    let (packets_sender, packets_receiver): (mpsc::Sender<stream::ReconstructedPackets>, mpsc::Receiver<stream::ReconstructedPackets>) = mpsc::channel();

    let handle = thread::spawn(move || {
        let n: usize = 5;
        let stride: usize = 1;
        let embedding_dim: i64 = 64;
        let hidden_dim: i64 = 32;
        let dropout: f64 = 0.2;

        let network_config = rnn::NetworkConfig::new(n, stride, embedding_dim, hidden_dim, dropout, rnn::RecurrentLayer::Lstm);
        let rnn_config: RNNConfig = Default::default();

        let recurrent_model = rnn::LSTMModel::new(network_config, rnn_config);
        let mut num_conn = 0;
        let threshold: f64 = 0.9; //static threshold for now. TODO: need to fix the way we define threshold

        loop {
            let ten_millis = std::time::Duration::from_millis(10);
            thread::sleep(ten_millis);
            continue;
            let data_received = srt_controller.get_ready_conn();
            //println!("Trying to get ready connection");
            match data_received {
                Some(reconstructed_packets) => {
                    rnn::save(&reconstructed_packets, &path);
                    if num_conn < num_training_conn {
                        recurrent_model.train_model_with_packet(&reconstructed_packets, stream::PacketDirection::Init);
                    }
                    else {
                        println!("Detecting,,,");
                        println!("{:?}", reconstructed_packets.get_init_tcp_message());
                        let is_benign = recurrent_model.detect_conn(&reconstructed_packets, stream::PacketDirection::Init, threshold);
                    }

                    num_conn += 1;
                },
                None => {}
            }
        }
    });

    handle.join();


    //let config = rnn::NetworkConfig::new(5, 64, 32, 0.2, rnn::RecurrentLayer::Lstm);
    //let rnn_config: RNNConfig = Default::default();

    //rnn::run(config, rnn_config);
    //println!("HOOOOOUUU");
    //rnn::preprocessing((0..20).collect(), 5, 1);

    //ctrlc::set_handler(move || {
    //    println!("received Ctrl+C!");
    //    reader_thread.stop_thread(&reader_handle);
    //})
    //.expect("Error setting Ctrl-C handler");
    //

    /*
    let path = env::args().nth(1).unwrap();
    let file_in = File::open(path).expect("Error opening file");
    let pcap_reader = PcapReader::new(file_in).unwrap();
    let pcap = pcap_reader;
    // Creates a new parser and parse the pcap header
    let mut src = &pcap[..];
    let (rem, pcap_parser) = PcapParser::new(&pcap[..]).unwrap();
    src = rem;

    loop {
        match pcap_parser.next_packet(src) {
            Ok((rem, packet)) => {
                let frame = EthernetFrame::new_unchecked(packet.data);

                src = rem;
                if rem.is_empty() {
                    break;
                }
            },
            Err(PcapError::IncompleteBuffer(needed)) => {},
            Err(_) => {}
        }
    }*/
}
