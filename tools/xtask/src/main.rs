#![forbid(unsafe_code)]

mod rshr_202_step_303_gate;
mod rshr_202_step_303_platform;

fn main() {
    if run().is_err() {
        eprintln!("step_303_gate_failed");
        std::process::exit(1);
    }
}

fn run() -> Result<(), ()> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("rshr-step-303-gate") => {
            let args = parse_gate_args(arguments.collect())?;
            rshr_202_step_303_gate::run(args).map_err(|_| ())
        }
        Some("rshr-step-303-platform-probe") if arguments.next().is_none() => {
            rshr_202_step_303_platform::run().map_err(|_| ())
        }
        _ => Err(()),
    }
}

fn parse_gate_args(values: Vec<String>) -> Result<rshr_202_step_303_gate::Arguments, ()> {
    let mut step = None;
    let mut check_id = None;
    let mut source_revision = None;
    let mut source_tree = None;
    let mut candidate_digest = None;
    let mut platform = None;
    let mut execution_request_sha256 = None;
    for value in values {
        let (name, value) = value.split_once('=').ok_or(())?;
        let slot = match name {
            "--check-id" => &mut check_id,
            "--source-revision" => &mut source_revision,
            "--source-tree" => &mut source_tree,
            "--candidate-digest" => &mut candidate_digest,
            "--platform" => &mut platform,
            "--execution-request-sha256" => &mut execution_request_sha256,
            "--step" => {
                let parsed = value.parse::<u16>().map_err(|_| ())?;
                if step.replace(parsed).is_some() {
                    return Err(());
                }
                continue;
            }
            _ => return Err(()),
        };
        if value.is_empty() || slot.replace(value.to_owned()).is_some() {
            return Err(());
        }
    }
    Ok(rshr_202_step_303_gate::Arguments {
        step: step.ok_or(())?,
        check_id: check_id.ok_or(())?,
        source_revision: source_revision.ok_or(())?,
        source_tree: source_tree.ok_or(())?,
        candidate_digest: candidate_digest.ok_or(())?,
        platform: platform.ok_or(())?,
        execution_request_sha256: execution_request_sha256.ok_or(())?,
    })
}
