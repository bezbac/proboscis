use testcontainers::{
    clients::{self, Cli},
    core::Port,
    images::{self, generic::WaitFor},
    Container, Docker, RunArgs,
};

pub fn start_dockerized_postgres<'a>(
    docker: &'a Cli,
) -> (
    String,
    Container<'a, clients::Cli, images::generic::GenericImage>,
) {
    let password = "password";

    let generic_postgres = images::generic::GenericImage::new("postgres:13.4-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", password);

    let node = docker.run_with_args(
        generic_postgres,
        RunArgs::default()
            .with_network("benchmark-network")
            .with_name("benchmark-postgres")
            .with_mapped_port(Port {
                local: 5432,
                internal: 5432,
            }),
    );

    let connection_string = format!("postgres://postgres:{}@0.0.0.0:{}/postgres", password, 5432,);

    (connection_string, node)
}
