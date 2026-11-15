use aeg_maia::process::{
    ControlFrameCodec, FrameKind, ListenerMetadata, WorkerMetadata, MAX_FRAME_BYTES,
};

#[test]
fn serialized_runtime_snapshot_can_be_read_by_worker() {
    let snapshot =
        aeg_maia::ConfigSnapshot::from_toml("[worker]\nprogram='/bin/worker'\ncount=2\n").unwrap();
    let encoded = toml::to_string(&snapshot).unwrap();
    let decoded = aeg_maia::ConfigSnapshot::from_toml(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
}

#[test]
fn encodes_and_decodes_versioned_length_delimited_frames() {
    let encoded = ControlFrameCodec::encode(FrameKind::Metadata, b"worker_id = 7\n").unwrap();
    assert_eq!(encoded[0], ControlFrameCodec::VERSION);
    assert_eq!(&encoded[2..6], &(14_u32.to_be_bytes()));

    let mut buffer = encoded;
    let frame = ControlFrameCodec::decode(&mut buffer).unwrap().unwrap();
    assert_eq!(frame.0, FrameKind::Metadata);
    assert_eq!(frame.1, b"worker_id = 7\n");
    assert!(buffer.is_empty());
}

#[test]
fn partial_frames_wait_until_the_payload_is_complete() {
    let encoded = ControlFrameCodec::encode(FrameKind::Ready, b"ready").unwrap();
    let mut buffer = encoded[..3].to_vec();
    assert!(ControlFrameCodec::decode(&mut buffer).unwrap().is_none());
    buffer.extend_from_slice(&encoded[3..]);
    assert_eq!(
        ControlFrameCodec::decode(&mut buffer).unwrap(),
        Some((FrameKind::Ready, b"ready".to_vec()))
    );
}

#[test]
fn rejects_wrong_versions_unknown_kinds_and_oversized_payloads() {
    let mut wrong_version = ControlFrameCodec::encode(FrameKind::Ready, &[]).unwrap();
    wrong_version[0] = ControlFrameCodec::VERSION + 1;
    assert!(ControlFrameCodec::decode(&mut wrong_version).is_err());

    let mut unknown_kind = vec![ControlFrameCodec::VERSION, 99, 0, 0, 0, 0];
    assert!(ControlFrameCodec::decode(&mut unknown_kind).is_err());

    assert!(ControlFrameCodec::encode(FrameKind::Config, &vec![0; MAX_FRAME_BYTES + 1]).is_err());
}

#[test]
fn metadata_carries_generation_worker_and_ordered_listener_fds() {
    let metadata = WorkerMetadata {
        generation_id: 9,
        worker_id: 2,
        listeners: vec![ListenerMetadata {
            name: "http.main".into(),
            fd: 17,
        }],
        entrypoint: Some("internal-worker".into()),
    };
    let encoded = toml::to_string(&metadata).unwrap();
    let decoded: WorkerMetadata = toml::from_str(&encoded).unwrap();
    assert_eq!(decoded, metadata);
}
