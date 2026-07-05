use libp2p::futures::{StreamExt as _, io, select};
use libp2p::swarm::SwarmEvent;
use std::hash::{Hash, Hasher};
use std::{error::Error, hash::DefaultHasher};
use std::time::Duration;
use libp2p::{gossipsub, request_response};

use tokio::io::BufReader;
use tokio::io::AsyncBufReadExt;
struct MyCodec;

#[derive(libp2p::swarm::NetworkBehaviour)]
struct Behaviour {
    mdns: libp2p::mdns::tokio::Behaviour,
    ping: libp2p::ping::Behaviour,
    gossipsub: libp2p::gossipsub::Behaviour
}

use tokio::io::stdin;

impl Behaviour {
    pub fn new(mdns: libp2p::mdns::tokio::Behaviour, ping: libp2p::ping::Behaviour, gossipsub: libp2p::gossipsub::Behaviour ) -> Self {
        Self { mdns, ping, gossipsub }
    }
}

#[tokio::main]
async fn main(
)->Result<(),Box<dyn Error>>{
	let mut swarm=libp2p::SwarmBuilder::with_new_identity()
		.with_tokio()
		.with_tcp(
			libp2p::tcp::Config::default(),
			libp2p::noise::Config::new,
			||libp2p::yamux::Config::default(),
		)?
		.with_behaviour(
			|keypair|{
				let mdns = libp2p::mdns::tokio::Behaviour::new(
					libp2p::mdns::Config::default(),
					keypair.public().into(),
				).unwrap();
                let ping = libp2p::ping::Behaviour::new(
                    libp2p::ping::Config::new()
                        .with_timeout(Duration::from_secs(5))
                        .with_interval(Duration::from_secs(1)),
                );
                let message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };
                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .message_id_fn(message_id_fn)
                    .build().unwrap();
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                    gossipsub_config, 
                ).unwrap();
                Behaviour::new(mdns, ping, gossipsub)
			}
		)?
        .with_swarm_config(|config|config.with_idle_connection_timeout(Duration::from_secs(5)))
		.build();

    // topic error 
    let topic = gossipsub::IdentTopic::new("test-net");
    swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();


    println!("peer id {}", swarm.local_peer_id());
	swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut lines_from_stdin = BufReader::new(stdin()).lines();
	loop{
        tokio::select! {
            line = lines_from_stdin.next_line() => {
                let line = line.unwrap();

                let res = swarm.behaviour_mut().gossipsub.publish(topic.clone(), line.unwrap().as_bytes());

                if let Err(e) = res {
                    println!("{}", e);
                }
            }   

            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: peer_id,
                    message_id: id,
                    message,
                })) => {
                    println!(
                        "Got message: {} with id: {} from peer: {}",
                        String::from_utf8_lossy(&message.data),
                        id,
                        peer_id
                    );
                }
                _ => {}
            }

            // ev = swarm.select_next_some() => {
            //     println!("{:#?}",ev);
            //     if let libp2p::swarm::SwarmEvent::Behaviour(BehaviourEvent::Mdns(libp2p::mdns::Event::Discovered(e))) =  ev{
            //         for (_peer_id, addr) in e {
            //             swarm.dial(addr)?;
            //         }
            //     }
            // }
        }
	}
}
