use aeg_maia::{ConfigSnapshot, ListenConfig, MasterConfig};

#[test]
fn parses_master_and_ordered_listener_configuration() {
    let snapshot = ConfigSnapshot::from_toml(
        r#"
        [master]
        pid_file = "/tmp/aeg-maia.pid"
        log_file = "/tmp/aeg-maia.log"
        daemon = true

        [worker]
        program = "/bin/worker"

        [[listen]]
        name = "http.main"
        address = "127.0.0.1:8080"

        [[listen]]
        name = "http.admin"
        address = "127.0.0.1:8081"
        "#,
    )
    .unwrap();

    assert_eq!(
        snapshot.master,
        MasterConfig {
            pid_file: Some("/tmp/aeg-maia.pid".into()),
            log_file: Some("/tmp/aeg-maia.log".into()),
            daemon: true,
        }
    );
    assert_eq!(
        snapshot.listeners,
        vec![
            ListenConfig {
                name: "http.main".into(),
                address: "127.0.0.1:8080".into(),
            },
            ListenConfig {
                name: "http.admin".into(),
                address: "127.0.0.1:8081".into(),
            },
        ]
    );
}

#[test]
fn rejects_duplicate_listener_names_and_empty_addresses() {
    let duplicate = ConfigSnapshot::from_toml(
        r#"
        [worker]
        program = "/bin/worker"
        [[listen]]
        name = "same"
        address = "127.0.0.1:8080"
        [[listen]]
        name = "same"
        address = "127.0.0.1:8081"
        "#,
    )
    .expect_err("duplicate listener names must fail");
    assert!(duplicate.to_string().contains("duplicate listener"));

    let empty = ConfigSnapshot::from_toml(
        r#"
        [worker]
        program = "/bin/worker"
        [[listen]]
        name = "empty"
        address = ""
        "#,
    )
    .expect_err("empty listener addresses must fail");
    assert!(empty.to_string().contains("address"));
}

#[test]
fn listener_identity_ignores_transient_worker_fds() {
    let mut first = ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\n[[listen]]\nname='main'\naddress='127.0.0.1:8080'\n",
    )
    .unwrap();
    let mut second = first.clone();
    first.worker.listener_fds = vec![3];
    second.worker.listener_fds = vec![99];
    assert_eq!(first.listener_identity(), second.listener_identity());
}

#[test]
fn rejects_unknown_master_and_listener_fields() {
    let error =
        ConfigSnapshot::from_toml("[master]\nunknown=true\n[worker]\nprogram='/bin/worker'\n")
            .expect_err("unknown master fields must fail");
    assert!(error.to_string().contains("unknown"));

    let error = ConfigSnapshot::from_toml(
        "[worker]\nprogram='/bin/worker'\n[[listen]]\nname='main'\naddress='127.0.0.1:8080'\nunknown=true\n",
    )
    .expect_err("unknown listener fields must fail");
    assert!(error.to_string().contains("unknown"));
}
