use libp2p::futures::StreamExt as _;
use std::error::Error;
use std::time::Duration;

#[derive(libp2p::swarm::NetworkBehaviour)]
struct Behaviour {
    mdns: libp2p::mdns::tokio::Behaviour,
    ping: libp2p::ping::Behaviour,
}

impl Behaviour {
    pub fn new(mdns: libp2p::mdns::tokio::Behaviour, ping: libp2p::ping::Behaviour) -> Self {
        Self { mdns, ping }
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
                Behaviour::new(mdns, ping)
			}
		)?
        .with_swarm_config(|config|config.with_idle_connection_timeout(Duration::from_secs(5)))
		.build();
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
