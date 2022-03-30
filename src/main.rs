use crate::node::LEAF_NODE_CELL_SIZE;
use crate::query::*;
use crate::table::*;
use std::io::Write;
use std::process::exit;

#[macro_use]
extern crate serde_big_array;
big_array! {
    BigArray;
    32, 255, LEAF_NODE_CELL_SIZE
}

#[macro_use]
extern crate log;

mod node;
mod query;
mod row;
mod table;
mod tree;

fn main() -> std::io::Result<()> {
    env_logger::init();

    let mut table = Table::new("data.db".to_string());
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        let output = handle_input(&mut table, &input);
        if output == "Exit" {
            table.flush();
            exit(0);
        }

        println!("{}", output);

        println!("Executed.");
        buffer.clear();
    }
}

fn print_prompt() {
    print!("db > ");
    let _ = std::io::stdout().flush();
}

fn handle_input(table: &mut Table, input: &str) -> String {
    if input.starts_with(".") {
        match handle_meta_command(&input) {
            MetaCommand::Exit => return "Exit".to_string(),
            MetaCommand::PrintTree => return table.to_string(),
            MetaCommand::Unrecognized => return format!("Unrecognized command '{input}'."),
        }
    }

    match prepare_statement(&input) {
        Ok(statement) => execute_statement(table, &statement),
        Err(reason) => reason,
    }
}

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn exit_command() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, ".exit");
        assert_eq!(output, "Exit");

        clean_test();
    }

    #[test]
    fn unrecognized_command() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, ".dfaskfd");
        assert_eq!(output, "Unrecognized command '.dfaskfd'.");

        clean_test();
    }

    #[test]
    fn invalid_statement() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, "insert 1 apple apple apple");
        assert_eq!(
            output,
            "Unrecognized keyword at start of 'insert 1 apple apple apple'."
        );

        clean_test();
    }

    #[test]
    fn select_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "select");
        assert_eq!(output, "");

        handle_input(&mut table, "insert 1 john john@email.com");
        handle_input(&mut table, "insert 2 wick wick@email.com");

        let output = handle_input(&mut table, "select");
        assert_eq!(
            output,
            "(1, john, john@email.com)\n(2, wick, wick@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn select_by_id_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "select 1");
        assert_eq!(output, "");

        handle_input(&mut table, "insert 1 john john@email.com");
        handle_input(&mut table, "insert 2 wick wick@email.com");

        let output = handle_input(&mut table, "select 1");
        assert_eq!(output, "(1, john, john@email.com)\n");

        let output = handle_input(&mut table, "select 2");
        assert_eq!(output, "(2, wick, wick@email.com)\n");

        clean_test();
    }

    #[test]
    fn insert_statement() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "insert 2 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 3 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 2...\n");

        clean_test();
    }

    #[test]
    fn insert_up_to_3_leaf_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..15 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        handle_input(&mut table, &format!("insert 15 user15 user15@email.com"));

        let expected_output = "- internal (size 1)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 8)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
    - 14
    - 15
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_up_to_4_leaf_node_split_when_child_max_key_larger_than_right_max_key() {
        let mut table = Table::new("test.db".to_string());
        let inputs = [
            "insert 18 user18 person18@example.com",
            "insert 7 user7 person7@example.com",
            "insert 10 user10 person10@example.com",
            "insert 29 user29 person29@example.com",
            "insert 23 user23 person23@example.com",
            "insert 4 user4 person4@example.com",
            "insert 14 user14 person14@example.com",
            "insert 30 user30 person30@example.com",
            "insert 15 user15 person15@example.com",
            "insert 26 user26 person26@example.com",
            "insert 22 user22 person22@example.com",
            "insert 19 user19 person19@example.com",
            "insert 2 user2 person2@example.com",
            "insert 1 user1 person1@example.com",
            "insert 21 user21 person21@example.com",
            "insert 11 user11 person11@example.com",
            "insert 6 user6 person6@example.com",
            "insert 20 user20 person20@example.com",
            "insert 5 user5 person5@example.com",
            "insert 8 user8 person8@example.com",
            "insert 9 user9 person9@example.com",
            "insert 3 user3 person3@example.com",
            "insert 12 user12 person12@example.com",
            "insert 27 user27 person27@example.com",
            "insert 17 user17 person17@example.com",
            "insert 16 user16 person16@example.com",
            "insert 13 user13 person13@example.com",
            "insert 24 user24 person24@example.com",
            "insert 25 user25 person25@example.com",
            "insert 28 user28 person28@example.com",
        ];

        for input in inputs {
            handle_input(&mut table, input);
        }

        let expected_output = "- internal (size 3)
  - leaf (size 7)
    - 1
    - 2
    - 3
    - 4
    - 5
    - 6
    - 7
  - key 7
  - leaf (size 8)
    - 8
    - 9
    - 10
    - 11
    - 12
    - 13
    - 14
    - 15
  - key 15
  - leaf (size 7)
    - 16
    - 17
    - 18
    - 19
    - 20
    - 21
    - 22
  - key 22
  - leaf (size 8)
    - 23
    - 24
    - 25
    - 26
    - 27
    - 28
    - 29
    - 30
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_up_to_4_leaf_node_split_when_child_max_key_not_larger_than_right_max_key() {
        let mut table = Table::new("test.db".to_string());
        let inputs = [
            "insert 1 user18 person18@example.com",
            "insert 4 user7 person7@example.com",
            "insert 7 user10 person10@example.com",
            "insert 10 user29 person29@example.com",
            "insert 13 user23 person23@example.com",
            "insert 14 user4 person4@example.com",
            "insert 19 user14 person14@example.com",
            "insert 24 user30 person30@example.com",
            "insert 27 user15 person15@example.com",
            "insert 30 user26 person26@example.com",
            "insert 40 user22 person22@example.com",
            "insert 55 user19 person19@example.com",
            "insert 41 user2 person2@example.com",
            "insert 34 user1 person1@example.com",
            "insert 21 user21 person21@example.com",
            "insert 60 user11 person11@example.com",
            "insert 64 user6 person6@example.com",
            "insert 58 user20 person20@example.com",
            "insert 76 user5 person5@example.com",
            "insert 88 user8 person8@example.com",
            "insert 90 user9 person9@example.com",
            "insert 70 user3 person3@example.com",
            "insert 5 user12 person12@example.com",
            "insert 2 user27 person27@example.com",
            "insert 72 user17 person17@example.com",
            "insert 66 user16 person16@example.com",
            "insert 53 user13 person13@example.com",
            "insert 34 user24 person24@example.com",
            "insert 22 user25 person25@example.com",
            "insert 23 user25 person25@example.com",
            "insert 25 user25 person25@example.com",
            "insert 26 user25 person25@example.com",
            "insert 28 user25 person25@example.com",
            "insert 31 user25 person25@example.com",
            "insert 32 user25 person25@example.com",
        ];

        for input in inputs {
            handle_input(&mut table, input);
        }

        let expected_output = "- internal (size 3)
  - leaf (size 9)
    - 1
    - 2
    - 4
    - 5
    - 7
    - 10
    - 13
    - 14
    - 19
  - key 19
  - leaf (size 7)
    - 21
    - 22
    - 23
    - 24
    - 25
    - 26
    - 27
  - key 27
  - leaf (size 7)
    - 28
    - 30
    - 31
    - 32
    - 34
    - 40
    - 41
  - key 41
  - leaf (size 11)
    - 53
    - 55
    - 58
    - 60
    - 64
    - 66
    - 70
    - 72
    - 76
    - 88
    - 90
";
        let output = table.to_string();
        assert_eq!(output, expected_output);
        clean_test()
    }

    #[test]
    fn insert_and_split_internal_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..36 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let expected_output = "- internal (size 1)
  - internal (size 2)
    - leaf (size 7)
      - 1
      - 2
      - 3
      - 4
      - 5
      - 6
      - 7
    - key 7
    - leaf (size 7)
      - 8
      - 9
      - 10
      - 11
      - 12
      - 13
      - 14
    - key 14
    - leaf (size 7)
      - 15
      - 16
      - 17
      - 18
      - 19
      - 20
      - 21
  - key 21
  - internal (size 1)
    - leaf (size 7)
      - 22
      - 23
      - 24
      - 25
      - 26
      - 27
      - 28
    - key 28
    - leaf (size 7)
      - 29
      - 30
      - 31
      - 32
      - 33
      - 34
      - 35
";
        let output = table.to_string();
        assert_eq!(output, expected_output);

        clean_test();
    }

    #[test]
    fn insert_string_at_max_length() {
        let mut table = Table::new("test.db".to_string());
        let mut username = String::new();
        for _ in 0..32 {
            username.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let mut email = String::new();
        for _ in 0..255 {
            email.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 2 john {email}"));
        assert_eq!(output, "inserting into page: 0, cell: 1...\n");

        clean_test();
    }

    #[test]
    fn error_when_duplicate_key() {
        let mut table = Table::new("test.db".to_string());

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "duplicate key\n");

        clean_test();
    }

    #[test]
    fn error_when_id_is_negative() {
        let mut table = Table::new("test.db".to_string());
        let output = handle_input(&mut table, "insert -1 john john@email.com");
        assert_eq!(output, "ID must be positive.");

        clean_test();
    }

    #[test]
    fn error_when_string_are_too_long() {
        let mut table = Table::new("test.db".to_string());
        let mut username = String::new();
        for _ in 0..33 {
            username.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "Name is too long.");

        let mut email = String::new();
        for _ in 0..256 {
            email.push_str("a");
        }

        let output = handle_input(&mut table, &format!("insert 1 john {email}"));
        assert_eq!(output, "Email is too long.");

        clean_test();
    }

    #[test]
    fn persist_data_to_file() {
        let mut table = Table::new("test.db".to_string());

        handle_input(&mut table, "insert 2 john john@email.com");
        handle_input(&mut table, "insert 1 wick wick@email.com");
        let output = handle_input(&mut table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );
        table.flush();

        let mut reopen_table = Table::new("test.db".to_string());
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn persist_leaf_and_internal_node_to_file() {
        let mut table = Table::new("test.db".to_string());
        let row_count = 1000;

        for i in 1..row_count {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        let expected_output: Vec<String> = (1..row_count)
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));

        // To test it doesn't go stack overflow.
        table.to_string();
        table.flush();

        let mut reopen_table = Table::new("test.db".to_string());
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(output, expected_output.join(""));

        clean_test();
    }

    #[test]
    // This is an edge case where previously our table_find method will return
    // Err("duplicate key") when an existed key is found. This work as expected when
    // we are using table_find for insertion.
    //
    // However, there's rare cases where 0 is part of the input, during select,
    // we called table_start, which then called table_find to find the 0 element to get
    // the first cell.
    //
    // Since the 0 element exist, instead of returning the cursor,
    // we return error, which our impl fallback to our previous incorrect
    // implementation and caused a panic as our code attempt to access cells that is empty.
    //
    // This test inputs is generated by quickcheck.
    fn edge_case_1() {
        let mut table = Table::new("test.db".to_string());
        let mut ids = vec![2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 0, 11, 12, 13];
        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    #[test]
    // Cases where we insert and split node, where the new node is not the last leaf node.
    //
    // The test inputs is generated by quickcheck.
    fn edge_case_2() {
        let mut table = Table::new("test.db".to_string());
        let mut ids: Vec<u32> = vec![
            1196709428, 2489455025, 2083637447, 4294967295, 3592348671, 2938449438, 3643979855,
            1049782310, 1363951140, 1346869668, 1601200172, 4041539161, 165331788, 1552149469,
            128342436, 2185737124, 1883182373, 958837483, 2012175646, 2275613780, 3987514949,
            3118733764, 1977365180, 0, 2504977491, 1645679146, 4089160664, 1257824002, 167856651,
            2219781630, 4024878278, 73472931, 1386688616, 2289910949, 1379355039, 3551564035,
            2882727650, 1732688862, 3660725099, 2358460733, 1285599636, 2452350314, 3176762246, 1,
            4259866189, 2254438901, 602185306, 2306766986, 3369680222, 2969005706, 668264387,
            3148942692,
        ];

        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    #[test]
    // Another test case that result in incorrect right_child_offset and parent_offset
    // being set by previous implementation.
    fn edge_case_3() {
        let mut table = Table::new("test.db".to_string());
        let mut ids: Vec<u32> = vec![
            4046930442, 3921144161, 4229192939, 3382240945, 4052938990, 2278855461, 2456473505,
            4064735575, 4207541631, 517463772, 1061340269, 1240903379, 2507590819, 1, 1717768101,
            2344446015, 813287232, 677509042, 3164314827, 2308630957, 712894876, 1386761012,
            1544312357, 1454467287, 1174258694, 0, 4061690588, 1605248421, 1629685041, 2914057616,
            3077557534, 2699076849, 1243126738, 455554470, 1406636693, 2156163181, 2576482160,
            1416480141, 824139726, 854776393, 3074268305, 919774497, 1910391461, 879293641,
            1059488243, 749046485, 3443907766, 717214207, 2102687613, 2122638882, 4294967295,
            1183710198, 1759045457, 3174766190, 3452935454, 1369486322, 1577951559, 1567643592,
            264882196, 303247237, 1858026677, 3717645950, 1595019133, 513208248, 740226363,
            129056044, 3940851756, 1403147878, 4139530007, 2651050044, 546554853, 1746449797,
            432385472,
        ];

        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    #[test]
    fn edge_case_4() {
        let mut table = Table::new("test.db".to_string());
        let mut ids: Vec<u32> = vec![
            1039039364, 449383650, 3689054439, 3990025253, 3648966878, 3488825869, 2953546758,
            777596548, 1317218180, 424356511, 1153045954, 4097353208, 2343327658, 588643681,
            2951690248, 2095641704, 2368624412, 1081741582, 3723295035, 1562100960, 1454457755,
            256657964, 361946241, 3149034577, 195728205, 2645088405, 2109029853, 821490685,
            424522005, 1342773334, 3242338732, 2558938407, 3988887356, 1722530320, 2444468120,
            1724941912, 3358270035, 2714393433, 2962742342, 2006877190, 1584660308, 2358898951,
            387665654, 34554432, 3373674698, 3335778813, 1770600923, 1514180148, 1076182609,
            3113332784, 3787710300, 1052039490, 1666523224, 2846179160, 4259816427, 538041855,
            559317427, 1979199020, 4086025766, 4159196363, 4241311689, 2874506549, 914724270,
            1927725625, 0, 1144246760, 3690958262, 714239470, 213414022, 4279487401, 655930002, 1,
            2747594677, 398902805, 1725501301,
        ];

        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    #[test]
    fn edge_case_5() {
        let mut table = Table::new("test.db".to_string());
        let mut ids: Vec<u32> = vec![
            4169718809, 4236538878, 3437930762, 100432263, 2984571246, 3966272303, 3275829974,
            1107248550, 3426658859, 385962272, 2221608141, 1305039362, 3965866038, 2362264656,
            2810931753, 1817926869, 3703243535, 472964308, 2003922755, 199729146, 2749263517,
            1989870548, 315394481, 1487028336, 3799867956, 4277994399, 4077576244, 3887847100,
            1767939938, 4090310902, 1275773529, 821731272, 2411065781, 429504110, 3080013801,
            3221194933, 2540328294, 574732466, 2884596891, 217180741, 831183990, 3084460986,
            3491586410, 1904693863, 250700765, 323175899, 3719578118, 1686939713, 1872170873,
            3308124420, 2517496895, 3095667251, 2881347613, 124171404, 4192754000, 33118690,
            1103893962, 746904435, 518068776, 3392166016, 0, 4210668953, 4225601389, 2695571929, 1,
            3610328721, 3471635988, 2880546981, 2086421747, 3092492214, 90907694, 2353126299,
            1964406623, 3642548797, 4294967295, 1822954304, 438006942, 3286180609, 2115727435,
            309471222, 4023894537, 1600736681, 1077483121, 2258733, 2780486638, 1226361602,
            458829584, 3713338081, 157754815, 1089651954, 952274728, 4029749492,
        ];
        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    #[test]
    fn edge_case_6() {
        let mut table = Table::new("test.db".to_string());
        let mut ids: Vec<u32> = vec![
            3382435867, 2781620200, 83106383, 4147853131, 1295290551, 4142895049, 4294967295,
            412794652, 2967371733, 0, 3986377166, 1240692179, 3487875282, 109365893, 1893622894,
            1040843281, 2072933767, 2915881961, 3876609394, 4012658875, 3660231275, 947237648,
            976717235, 1846912049, 108529937, 1708939796, 374168883, 233776229, 2294156580,
            3219467422, 3638811430, 4057803256, 961600890, 2295025637, 1294063577, 660458214,
            307368866, 377935319, 655048382, 3347222051, 2282802440, 772162491, 2715790627,
            4238258251, 4025516826, 1537039460, 3527259625, 2696366718, 2386640490, 2042506169,
            15160950, 2498648450, 879945756, 277308937, 1739326107, 1405635068, 3964009246,
            1829670428, 947381889, 1, 3896555183, 2633704506, 609498228, 3349407468, 1991619512,
            1160238434, 2593998749, 584287087, 237492343, 2921247223, 2097760467, 2996056874,
            3300871123, 2011205031, 1464349335, 3317378212, 3078650142, 3578010797, 732156332,
            207540948, 493991125,
        ];

        for i in &ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "select");
        ids.sort();

        let expected_output: Vec<String> = ids
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect();

        assert_eq!(output, expected_output.join(""));
    }

    use quickcheck::{Arbitrary, Gen};
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    #[derive(Clone, Debug)]
    struct UniqueIDs(Vec<u32>);

    #[derive(Clone, Debug)]
    struct DeleteInputs {
        pub insertion_ids: Vec<u8>,
        pub deletion_ids: Vec<u8>,
    }

    impl Arbitrary for UniqueIDs {
        fn arbitrary(g: &mut Gen) -> UniqueIDs {
            let mut vec = Vec::<u32>::arbitrary(g);
            vec.sort();
            vec.dedup();
            vec.shuffle(&mut thread_rng());
            UniqueIDs(vec)
        }
    }

    impl Arbitrary for DeleteInputs {
        fn arbitrary(g: &mut Gen) -> DeleteInputs {
            let mut insertion_ids = Vec::<u8>::arbitrary(g);
            insertion_ids.sort();
            insertion_ids.dedup();
            insertion_ids.shuffle(&mut thread_rng());

            let mut deletion_ids = insertion_ids.clone();
            deletion_ids.shuffle(&mut thread_rng());

            Self {
                insertion_ids,
                deletion_ids,
            }
        }
    }

    quickcheck! {
        fn insert_and_select_prop(ids: UniqueIDs) -> bool {
            let mut table = Table::new("test.db".to_string());

            for i in &ids.0 {
                handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
            }

            let output = handle_input(&mut table, "select");

            let mut sorted_ids = ids.0.clone();
            sorted_ids.sort();
            let expected_output: Vec<String> = sorted_ids
                .iter()
                .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
                .collect();

            let result = output == expected_output.join("");
            result
        }
    }

    #[test]
    fn delete_row_from_tree_with_only_root_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..10 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "delete 5");
        assert_eq!(output, "deleted 5");

        let output = handle_input(&mut table, "select 5");
        assert_eq!(output, "");

        let output = handle_input(&mut table, "select");
        let expected_output = [1, 2, 3, 4, 6, 7, 8, 9]
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("");

        assert_eq!(output, expected_output);
    }

    #[test]
    fn delete_row_from_tree_with_2_level_internal_and_leaf_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..20 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "delete 5");
        assert_eq!(output, "deleted 5");

        let output = handle_input(&mut table, "select 5");
        assert_eq!(output, "");

        let output = handle_input(&mut table, "select");
        let expected_output = (1..20)
            .filter(|&i| i != 5)
            .collect::<Vec<u32>>()
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("");

        assert_eq!(output, expected_output);
    }

    #[test]
    fn delete_row_from_tree_with_3_level_internal_and_leaf_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..100 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "delete 5");
        assert_eq!(output, "deleted 5");

        let output = handle_input(&mut table, "delete 90");
        assert_eq!(output, "deleted 90");

        let output = handle_input(&mut table, "delete 55");
        assert_eq!(output, "deleted 55");

        let output = handle_input(&mut table, "select");
        let expected_output = (1..100)
            .filter(|&i| i != 5 && i != 90 && i != 55)
            .collect::<Vec<u32>>()
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("");

        assert_eq!(output, expected_output);
    }

    #[test]
    fn delete_row_with_id_in_internal_node() {
        let mut table = Table::new("test.db".to_string());

        for i in 1..100 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "delete 7");
        assert_eq!(output, "deleted 7");

        let output = handle_input(&mut table, "select");
        let expected_output = (1..100)
            .filter(|&i| i != 7)
            .collect::<Vec<u32>>()
            .iter()
            .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
            .collect::<Vec<String>>()
            .join("");

        assert_eq!(output, expected_output);

        let output = handle_input(&mut table, &format!("insert 7 user7 user7@email.com"));
        assert_eq!(output, "inserting into page: 1, cell: 6...\n");
    }

    #[test]
    fn delete_everything() {
        let mut table = Table::new("test.db".to_string());

        for i in [1, 100] {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        let output = handle_input(&mut table, "delete 1");
        assert_eq!(output, "deleted 1");

        let output = handle_input(&mut table, "delete 100");
        assert_eq!(output, "deleted 100");

        let output = handle_input(&mut table, "select");
        assert_eq!(output, "");

        handle_input(&mut table, "insert 7 user7 user7@email.com");
        let output = handle_input(&mut table, "select");
        assert_eq!(output, "(7, user7, user7@email.com)\n");
    }

    #[test]
    fn delete_test_case_1() {
        env_logger::init();
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                99, 209, 83, 115, 33, 1, 180, 91, 82, 255, 74, 78, 178, 190, 139, 0, 51, 164, 72,
                93, 170, 100, 244, 198, 69,
            ],
            deletion_ids: vec![
                139, 82, 51, 1, 83, 93, 69, 170, 244, 72, 33, 99, 180, 190, 74, 78, 100, 115, 209,
                164, 178, 91, 0, 255, 198,
            ],
        };

        let mut table = Table::new("test.db".to_string());

        for i in &delete_input.insertion_ids {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        for i in &delete_input.deletion_ids {
            let output = handle_input(&mut table, &format!("delete {i}"));
            assert_eq!(output, format!("deleted {i}"));

            println!("{}", table.to_string());
            let output = handle_input(&mut table, "select");
            let mut sorted_ids = delete_input.insertion_ids.clone();
            sorted_ids.sort();

            let index = delete_input
                .deletion_ids
                .iter()
                .position(|id| id == i)
                .unwrap();

            let expected_output = sorted_ids
                .iter()
                .filter(|&id| {
                    if index > 0 {
                        !delete_input.deletion_ids[0..index + 1].contains(id)
                    } else {
                        id != i
                    }
                })
                .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
                .collect::<Vec<String>>()
                .join("");

            assert_eq!(output, expected_output)
        }
    }

    quickcheck! {
        fn insert_delete_and_select_prop(delete_input: DeleteInputs) -> bool {
            let mut table = Table::new("test.db".to_string());

            for i in &delete_input.insertion_ids {
                handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
            }

            for i in &delete_input.deletion_ids {
                let output = handle_input(&mut table, &format!("delete {i}"));
                assert_eq!(output, format!("deleted {i}"));

                let output = handle_input(&mut table, "select");
                let mut sorted_ids = delete_input.insertion_ids.clone();
                sorted_ids.sort();

                let index = delete_input
                    .deletion_ids
                    .iter()
                    .position(|id| id == i)
                    .unwrap();

                let expected_output = sorted_ids
                    .iter()
                    .filter(|&id| {
                        if index > 0 {
                            !delete_input.deletion_ids[0..index + 1].contains(id)
                        } else {
                            id != i
                        }
                    })
                    .map(|i| format!("({i}, user{i}, user{i}@email.com)\n"))
                    .collect::<Vec<String>>()
                    .join("");

                if output == expected_output {
                    continue;
                } else {
                    return false;
                }
            }

            return true
        }
    }

    fn clean_test() {
        let _ = std::fs::remove_file("test.db");
    }
}
