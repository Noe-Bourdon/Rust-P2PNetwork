use libp2p::futures::{AsyncBufReadExt, StreamExt as _, io};
use std::hash::{Hash, Hasher};
use std::{error::Error, hash::DefaultHasher};
use std::time::Duration;
use libp2p::{gossipsub, request_response};
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

    let mut stdin = io::BufReader::new(io::stdin()).lines();

    println!("peer id {}", swarm.local_peer_id());
	swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
	loop{
        let ev = swarm.select_next_some().await;
        println!("{:#?}",ev);
        if let libp2p::swarm::SwarmEvent::Behaviour(BehaviourEvent::Mdns(libp2p::mdns::Event::Discovered(e))) =  ev{
            for (_peer_id, addr) in e {
                swarm.dial(addr)?;
            }
        }
	}
}
