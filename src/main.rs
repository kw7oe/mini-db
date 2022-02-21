use std::io::Write;

fn main() -> std::io::Result<()> {
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        if input.eq(".exit") {
            return Ok(());
        } else {
            println!("Unrecognized command '{input}'.");
        }
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = std::io::stdout().flush();
}
