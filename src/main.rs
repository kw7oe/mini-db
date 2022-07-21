use crate::query::*;
use crate::storage::LEAF_NODE_CELL_SIZE;
use crate::table::*;
use std::io::Write;
use std::process::exit;

#[macro_use]
extern crate serde_big_array;
big_array! {
    BigArray;
    32, 255, LEAF_NODE_CELL_SIZE
}

mod concurrency;
mod query;
mod recovery;
mod row;
mod storage;
mod table;

fn main() -> std::io::Result<()> {
    let mut table = Table::new("data.db", 8);
    let mut buffer = String::new();

    loop {
        print_prompt();
        std::io::stdin().read_line(&mut buffer)?;

        let input = buffer.trim();
        let output = handle_input(&mut table, input);
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
    if input.starts_with('.') {
        match handle_meta_command(input) {
            MetaCommand::Exit => return "Exit".to_string(),
            MetaCommand::PrintTree => return table.to_string(),
            MetaCommand::PrintPages => return table.pages(),
            MetaCommand::Unrecognized => return format!("Unrecognized command '{input}'."),
        }
    }

    match prepare_statement(input) {
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
        let mut table = setup_test_table();
        let output = handle_input(&mut table, ".exit");
        assert_eq!(output, "Exit");

        clean_test();
    }

    #[test]
    fn unrecognized_command() {
        let mut table = setup_test_table();
        let output = handle_input(&mut table, ".dfaskfd");
        assert_eq!(output, "Unrecognized command '.dfaskfd'.");

        clean_test();
    }

    #[test]
    fn invalid_statement() {
        let mut table = setup_test_table();
        let output = handle_input(&mut table, "insert 1 apple apple apple");
        assert_eq!(
            output,
            "Unrecognized keyword at start of '1 apple apple apple'."
        );

        clean_test();
    }

    #[test]
    fn select_statement() {
        let mut table = setup_test_table();

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
        let mut table = setup_test_table();

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
        let mut table = setup_test_table();

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
        let mut table = setup_test_table();

        for i in 1..15 {
            handle_input(&mut table, &format!("insert {i} user{i} user{i}@email.com"));
        }

        handle_input(&mut table, "insert 15 user15 user15@email.com");

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
        let mut table = setup_test_table();
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
        let mut table = setup_test_table();
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
        let mut table = setup_test_table();

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
        let mut table = setup_test_table();
        let mut username = String::new();
        for _ in 0..32 {
            username.push('a');
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let mut email = String::new();
        for _ in 0..255 {
            email.push('a');
        }

        let output = handle_input(&mut table, &format!("insert 2 john {email}"));
        assert_eq!(output, "inserting into page: 0, cell: 1...\n");

        clean_test();
    }

    #[test]
    fn error_when_duplicate_key() {
        let mut table = setup_test_table();

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "inserting into page: 0, cell: 0...\n");

        let output = handle_input(&mut table, "insert 1 john john@email.com");
        assert_eq!(output, "duplicate key\n");

        clean_test();
    }

    #[test]
    fn error_when_id_is_negative() {
        let mut table = setup_test_table();
        let output = handle_input(&mut table, "insert -1 john john@email.com");
        assert_eq!(output, "invalid id provided");

        clean_test();
    }

    #[test]
    fn error_when_string_are_too_long() {
        let mut table = setup_test_table();
        let mut username = String::new();
        for _ in 0..33 {
            username.push('a');
        }

        let output = handle_input(&mut table, &format!("insert 1 {username} john@email.com"));
        assert_eq!(output, "Name is too long.");

        let mut email = String::new();
        for _ in 0..256 {
            email.push('a');
        }

        let output = handle_input(&mut table, &format!("insert 1 john {email}"));
        assert_eq!(output, "Email is too long.");

        clean_test();
    }

    #[test]
    fn persist_data_to_file() {
        let mut table = setup_test_table();

        handle_input(&mut table, "insert 2 john john@email.com");
        handle_input(&mut table, "insert 1 wick wick@email.com");
        let output = handle_input(&mut table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );
        table.flush();

        let mut reopen_table = setup_test_table();
        let output = handle_input(&mut reopen_table, "select");
        assert_eq!(
            output,
            "(1, wick, wick@email.com)\n(2, john, john@email.com)\n"
        );

        clean_test();
    }

    #[test]
    fn persist_leaf_and_internal_node_to_file() {
        let mut table = setup_test_table();
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
        // table.to_string();
        table.flush();

        let mut reopen_table = setup_test_table();
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
    fn insert_edge_case_1() {
        let ids = vec![2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 0, 11, 12, 13];
        test_insertion(ids);
    }

    #[test]
    // Cases where we insert and split node, where the new node is not the last leaf node.
    //
    // The test inputs is generated by quickcheck.
    fn insert_edge_case_2() {
        let ids: Vec<u32> = vec![
            1196709428, 2489455025, 2083637447, 4294967295, 3592348671, 2938449438, 3643979855,
            1049782310, 1363951140, 1346869668, 1601200172, 4041539161, 165331788, 1552149469,
            128342436, 2185737124, 1883182373, 958837483, 2012175646, 2275613780, 3987514949,
            3118733764, 1977365180, 0, 2504977491, 1645679146, 4089160664, 1257824002, 167856651,
            2219781630, 4024878278, 73472931, 1386688616, 2289910949, 1379355039, 3551564035,
            2882727650, 1732688862, 3660725099, 2358460733, 1285599636, 2452350314, 3176762246, 1,
            4259866189, 2254438901, 602185306, 2306766986, 3369680222, 2969005706, 668264387,
            3148942692,
        ];
        test_insertion(ids);
    }

    #[test]
    // Another test case that result in incorrect right_child_offset and parent_offset
    // being set by previous implementation.
    fn insert_edge_case_3() {
        let ids: Vec<u32> = vec![
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
        test_insertion(ids);
    }

    #[test]
    fn insert_edge_case_4() {
        let ids: Vec<u32> = vec![
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
        test_insertion(ids);
    }

    #[test]
    fn insert_edge_case_5() {
        let ids: Vec<u32> = vec![
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
        test_insertion(ids);
    }

    #[test]
    fn insert_edge_case_6() {
        let ids: Vec<u32> = vec![
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
        test_insertion(ids);
    }

    #[test]
    fn insert_edge_case_7() {
        let ids = vec![
            56332, 21075, 27212, 0, 12173, 6529, 32739, 213, 2149, 29259, 25273, 11219, 48995,
            13431, 9044, 9631, 36790, 55789, 54583, 64809, 42177, 53379, 12789, 8475, 56135, 65535,
            11845, 32939, 64547, 38360, 25285, 26122, 33617, 9480, 32017, 15137, 28420, 56542,
            32026, 7666, 42299, 52238, 2909, 6344, 870, 20574, 46493, 14776, 60178, 41085, 65274,
            783, 13739, 39586, 11499, 44617, 52467, 19804, 35942, 50350, 19024, 40721, 58164,
            41820, 1, 1242, 1227, 7154, 62297, 22630, 2468, 5527, 30697, 61351, 24471, 13585,
            52156, 10271, 39571, 22784, 50625, 38573, 47947, 7079, 47963, 58296, 38350, 25982,
        ];
        test_insertion(ids);
    }
    #[test]
    fn insert_and_split_internal_node_update_parent_child_pointers_correctly() {
        let ids = vec![
            60898, 22824, 62638, 31229, 35487, 25977, 24093, 17004, 15352, 15827, 25239, 48616,
            53477, 28012, 51209, 12553, 61094, 2628, 16919, 1748, 6893, 10645, 64350, 54423, 0,
            61038, 26619, 2331, 63334, 33243, 54921, 62595, 60846, 21040, 28490, 41360, 21638,
            63235, 43692, 913, 60694, 55014, 6601, 18620, 41899, 57726, 49591, 14888, 1, 34660,
            65021, 59085, 32077, 34899, 53759, 44187, 3357, 59023, 55551, 39636, 24887, 45861,
            48083, 53066, 36098, 23066, 45313, 59531, 42323, 26707, 43939, 61652, 59494, 3543,
            21267, 50003, 55859, 34882, 12936, 47979, 34012, 31995, 4244, 32082, 12517, 9915,
            65535, 12147, 40738,
        ];
        test_insertion(ids);
    }

    fn test_insertion<T: std::fmt::Display + Ord>(mut ids: Vec<T>) {
        let mut table = setup_test_table();
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
        clean_test();
    }

    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    #[derive(Clone, Debug)]
    struct UniqueIDs(pub Vec<u16>);

    #[derive(Clone, Debug)]
    struct DeleteInputs {
        pub insertion_ids: Vec<u8>,
        pub deletion_ids: Vec<u8>,
    }

    impl Arbitrary for UniqueIDs {
        fn arbitrary(g: &mut Gen) -> UniqueIDs {
            let mut vec = Vec::<u16>::arbitrary(g);
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
            let mut table = setup_test_table();

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
            clean_test();
            result
        }
    }

    #[test]
    fn delete_row_from_tree_with_only_root_node() {
        let mut table = setup_test_table();

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

        clean_test();
    }

    #[test]
    fn delete_row_from_tree_with_2_level_internal_and_leaf_node() {
        let mut table = setup_test_table();

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

        clean_test();
    }

    #[test]
    fn delete_row_from_tree_with_3_level_internal_and_leaf_node() {
        let mut table = setup_test_table();

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

        clean_test();
    }

    #[test]
    fn delete_row_with_id_in_internal_node() {
        let mut table = setup_test_table();

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

        let output = handle_input(&mut table, "insert 7 user7 user7@email.com");
        assert_eq!(output, "inserting into page: 1, cell: 6...\n");

        clean_test();
    }

    #[test]
    fn delete_everything() {
        let mut table = setup_test_table();

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

        clean_test();
    }

    #[test]
    fn delete_and_merge_leaf_nodes_with_left_neighbour_and_promote_to_root_node() {
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
        test_deletion(delete_input);
    }

    #[test]
    fn delete_and_merge_leaf_node_with_right_neighbour() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                255, 17, 43, 99, 182, 183, 88, 90, 247, 184, 104, 240, 39, 96, 205, 164, 2, 51,
                224, 78, 82, 219, 35, 28, 190, 188, 100, 26, 42, 192, 147, 159, 199, 77, 237, 185,
                61, 108, 69, 54, 112, 186,
            ],
            deletion_ids: vec![
                112, 35, 190, 104, 219, 90, 42, 237, 69, 185, 240, 199, 182, 247, 108, 205, 54,
                159, 39, 224, 184, 28, 43, 99, 192, 26, 2, 77, 17, 183, 186, 88, 96, 78, 61, 51,
                147, 255, 188, 164, 82, 100,
            ],
        };
        test_deletion(delete_input);
    }

    #[test]
    fn delete_and_merge_internal_node_with_left_neighbour() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                22, 242, 82, 113, 216, 62, 147, 43, 135, 105, 230, 183, 65, 111, 121, 174, 109,
                116, 114, 205, 64, 71, 73, 201, 1, 238, 252, 228, 154, 192, 246, 107, 218, 56, 232,
                206, 176, 142, 118, 255, 8, 136, 249, 10, 175, 191, 165, 4, 16, 25, 17, 31, 9, 0,
                130,
            ],
            deletion_ids: vec![
                191, 8, 116, 16, 0, 154, 121, 130, 135, 113, 238, 71, 192, 31, 242, 9, 10, 165,
                206, 201, 118, 109, 136, 174, 255, 205, 64, 176, 22, 1, 56, 73, 175, 4, 230, 65,
                43, 232, 142, 228, 17, 25, 147, 246, 62, 114, 82, 107, 111, 249, 216, 105, 183,
                218, 252,
            ],
        };
        test_deletion(delete_input);
    }

    #[test]
    fn delete_and_merge_right_most_internal_nodes_with_parent_updated() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                247, 0, 195, 91, 239, 86, 18, 97, 161, 17, 111, 62, 152, 180, 116, 199, 96, 65,
                254, 45, 242, 56, 8, 34, 127, 243, 105, 7, 238, 1, 225, 60, 249, 37, 228, 108, 49,
                19, 104, 255, 138, 189, 126, 241, 136, 36, 202, 87, 121, 64, 184, 144, 176, 196,
                220, 94, 4, 41, 58, 150, 237, 146, 77, 251, 236, 114, 99, 14, 90, 210, 101, 171,
                160, 148,
            ],
            deletion_ids: vec![
                251, 152, 199, 180, 91, 19, 161, 4, 41, 7, 148, 65, 99, 105, 1, 45, 138, 126, 8,
                210, 171, 228, 127, 255, 243, 160, 90, 114, 195, 111, 136, 254, 242, 64, 247, 196,
                56, 249, 236, 96, 220, 36, 87, 146, 101, 108, 18, 34, 237, 239, 144, 14, 238, 189,
                241, 150, 0, 121, 104, 17, 176, 184, 202, 60, 94, 86, 62, 77, 97, 58, 49, 116, 225,
                37,
            ],
        };

        test_deletion(delete_input);
    }

    #[test]
    fn delete_and_merge_internal_nodes() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                107, 202, 123, 47, 49, 89, 174, 240, 10, 24, 162, 0, 201, 228, 114, 189, 38, 16,
                219, 32, 211, 229, 176, 143, 118, 91, 214, 142, 191, 172, 99, 7, 253, 52, 188, 177,
                121, 33, 194, 236, 244, 132, 120, 252, 231, 134, 1, 39, 117, 217, 196, 87, 96, 23,
                230, 11, 12, 154, 48, 131, 70, 61, 111, 255, 184, 71, 21, 26, 155, 235, 67, 139,
                90, 57,
            ],
            deletion_ids: vec![
                24, 217, 121, 111, 67, 48, 16, 21, 57, 132, 177, 114, 10, 11, 202, 0, 139, 155, 12,
                120, 39, 236, 219, 32, 71, 211, 229, 154, 176, 174, 201, 38, 143, 191, 244, 253,
                52, 189, 162, 118, 172, 240, 91, 131, 107, 142, 231, 89, 7, 188, 196, 255, 26, 23,
                252, 87, 61, 70, 123, 90, 117, 214, 33, 230, 134, 184, 96, 194, 49, 99, 1, 228, 47,
                235,
            ],
        };
        test_deletion(delete_input);
    }

    #[test]
    // This test case are able to catch bug due to incorrect parent offset of
    // our B+ Tree node, as it will trigger merging on leaf node that have incorrect
    // parent offset.
    fn delete_and_merge_internal_nodes_with_parent_updated() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                113, 175, 203, 91, 229, 214, 149, 46, 8, 195, 112, 156, 205, 171, 223, 226, 138,
                59, 134, 97, 248, 179, 161, 22, 90, 197, 1, 17, 9, 132, 129, 11, 109, 44, 63, 150,
                42, 141, 29, 162, 184, 89, 120, 173, 100, 2, 183, 133, 199, 62, 194, 255, 14, 80,
                110, 231, 121, 13, 98, 10, 108, 225, 174, 93, 177, 64, 84, 21, 86, 126, 27, 76, 25,
                0, 77, 85,
            ],
            deletion_ids: vec![
                11, 134, 93, 141, 161, 126, 85, 205, 174, 46, 199, 9, 179, 8, 171, 248, 109, 97, 2,
                225, 10, 64, 183, 42, 0, 156, 149, 100, 120, 17, 184, 21, 231, 138, 108, 203, 150,
                121, 255, 1, 14, 98, 44, 84, 110, 77, 214, 129, 229, 194, 13, 90, 162, 27, 86, 89,
                195, 112, 76, 22, 177, 133, 62, 175, 113, 197, 25, 226, 59, 63, 132, 173, 80, 223,
                29, 91,
            ],
        };

        test_deletion(delete_input);
    }

    #[test]
    // This test catch updating children parent offset on an non existing index.
    fn delete_and_merge_internal_nodes_while_updating_children_parent_offset() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                190, 63, 111, 89, 20, 8, 71, 160, 13, 199, 224, 103, 255, 9, 179, 214, 38, 218, 52,
                128, 11, 157, 39, 215, 191, 231, 50, 205, 53, 1, 7, 124, 74, 48, 69, 57, 84, 237,
                123, 136, 130, 46, 120, 37, 234, 80, 0, 72, 183, 206, 5, 78, 175, 165, 106, 3, 242,
                2, 56, 153, 243, 177, 144, 246, 171, 140, 70, 184, 126, 163, 98, 145, 239, 188,
            ],
            deletion_ids: vec![
                255, 177, 72, 124, 175, 183, 1, 243, 231, 8, 144, 50, 163, 11, 218, 78, 46, 106,
                171, 13, 20, 74, 214, 140, 80, 0, 5, 234, 98, 53, 224, 205, 120, 165, 52, 123, 48,
                63, 70, 239, 37, 184, 145, 199, 38, 39, 84, 3, 126, 188, 136, 128, 56, 69, 153, 71,
                206, 57, 9, 130, 160, 190, 103, 157, 7, 111, 191, 246, 242, 2, 237, 89, 215, 179,
            ],
        };

        test_deletion(delete_input);
    }

    #[test]
    // This test case catch not updating children parent offset on existing nodes
    // that is affected by the merging process. It happen when we remove a node in
    // the middle of our internal nodes, and caused the children of the nodes after
    // the removed node not having their parent offset updated.
    fn delete_and_merge_internal_nodes_while_updating_old_right_cp_parent_offset() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                137, 71, 81, 209, 0, 90, 235, 141, 208, 110, 178, 241, 160, 111, 63, 245, 246, 255,
                91, 147, 70, 74, 139, 229, 26, 161, 57, 51, 146, 34, 94, 7, 8, 114, 221, 25, 164,
                227, 252, 186, 15, 118, 173, 250, 203, 59, 187, 41, 183, 14, 33, 99, 215, 1, 191,
                177, 213, 130, 222, 176, 202, 192, 93, 103, 199, 6, 67, 184,
            ],
            deletion_ids: vec![
                34, 160, 26, 67, 118, 41, 6, 209, 7, 91, 14, 199, 103, 139, 141, 99, 93, 1, 81, 33,
                137, 177, 90, 70, 164, 0, 203, 184, 57, 250, 252, 74, 110, 221, 186, 255, 147, 191,
                111, 245, 71, 114, 94, 146, 202, 161, 241, 192, 183, 176, 63, 235, 229, 246, 15,
                227, 222, 8, 173, 51, 25, 59, 208, 187, 213, 215, 178, 130,
            ],
        };

        test_deletion(delete_input)
    }

    #[test]
    // This test case catch not updating children parent offset on existing nodes
    // because we doesn't update those nodes that is affected by the removal of a most right child
    // that happen to have a smaller index than the other child
    fn delete_and_merge_internal_nodes_while_updating_affected_node() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                84, 145, 87, 203, 146, 95, 132, 17, 253, 133, 77, 125, 105, 56, 1, 101, 180, 218,
                110, 48, 49, 239, 112, 52, 0, 138, 191, 126, 252, 217, 171, 172, 64, 104, 147, 71,
                219, 16, 150, 255, 75, 210, 166, 66, 25, 96, 53, 178, 168, 243, 41, 67, 176, 188,
                137, 209, 5, 40, 246, 197, 92, 165, 63, 190, 32, 151, 70, 205, 195,
            ],
            deletion_ids: vec![
                132, 205, 52, 168, 218, 180, 243, 188, 190, 151, 146, 195, 105, 219, 17, 171, 101,
                217, 112, 84, 40, 203, 70, 210, 147, 239, 48, 25, 92, 0, 5, 191, 197, 53, 125, 16,
                255, 178, 126, 252, 150, 95, 56, 63, 166, 246, 66, 176, 253, 133, 64, 172, 104, 87,
                71, 77, 96, 110, 49, 75, 145, 1, 41, 32, 137, 209, 67, 165, 138,
            ],
        };

        test_deletion(delete_input);
    }

    #[test]
    // This test case catch the issue wheere when left node is empty and right node has the max
    // count, it doesn't get merge when it's supposed to.
    fn delete_and_merge_internal_nodes_when_right_and_left_equal_to_max_internal_count() {
        let delete_input = DeleteInputs {
            insertion_ids: vec![
                86, 155, 221, 150, 138, 178, 141, 61, 251, 204, 212, 127, 12, 22, 157, 182, 225,
                164, 66, 208, 25, 103, 65, 70, 21, 207, 55, 253, 29, 72, 240, 133, 135, 144, 222,
                23, 186, 248, 75, 115, 167, 180, 31, 226, 174, 205, 47, 89, 110, 53, 220, 121, 51,
                129, 159, 254, 99, 42, 11, 5, 187, 239, 10, 184, 154, 160, 219, 94, 91, 96, 136,
                40, 28, 117, 97, 193, 100, 30, 95, 223, 13, 98, 241, 146, 105, 134, 83, 189, 143,
                177, 250, 58, 37, 60, 34, 27, 20, 137, 191, 198, 197, 249, 79, 76, 14, 238, 201,
                63, 202, 4, 16, 181, 175, 218, 38, 199, 19, 3, 168, 228, 122, 57, 161, 54, 142, 69,
                74, 17, 48, 230, 170, 242, 128, 118, 39, 125, 41, 123, 206, 84, 62, 194, 149, 33,
                139, 188, 116, 176, 183, 56, 43, 44, 85, 243, 148, 165, 203, 102, 185, 233, 169,
                156, 87, 163, 236, 192, 46, 227, 112, 252, 166, 255, 0, 244, 247, 24, 7, 158, 26,
                80, 18, 211, 15, 217, 124, 32, 71, 215, 77, 82, 49, 140, 209, 214, 114, 107, 90,
                45, 104, 145, 93, 132, 216, 36, 130, 196, 1, 52, 108, 50, 88, 111, 109, 235, 195,
                67, 153, 8, 151, 78, 162, 92, 131, 68, 229, 172, 106, 81, 237, 59, 245, 113, 231,
                210,
            ],
            deletion_ids: vec![
                122, 143, 188, 17, 81, 210, 60, 174, 159, 247, 79, 91, 205, 87, 28, 124, 156, 76,
                19, 167, 254, 141, 218, 112, 242, 41, 110, 57, 90, 10, 169, 236, 175, 93, 49, 201,
                51, 151, 66, 187, 54, 115, 85, 127, 0, 99, 33, 132, 5, 178, 214, 42, 192, 161, 220,
                43, 191, 249, 177, 96, 176, 189, 184, 193, 136, 228, 239, 206, 63, 252, 165, 251,
                70, 207, 170, 31, 39, 131, 77, 130, 117, 211, 50, 14, 241, 55, 13, 182, 202, 237,
                253, 56, 248, 100, 89, 95, 172, 80, 148, 186, 106, 123, 223, 121, 27, 153, 44, 240,
                37, 53, 212, 45, 88, 23, 75, 103, 180, 164, 62, 160, 222, 215, 84, 3, 225, 135,
                181, 139, 111, 105, 16, 18, 61, 229, 155, 145, 11, 38, 30, 250, 20, 8, 113, 243,
                48, 199, 24, 204, 34, 71, 83, 46, 104, 233, 221, 226, 146, 40, 162, 94, 197, 97,
                194, 255, 125, 7, 216, 134, 86, 238, 133, 245, 68, 72, 203, 109, 26, 65, 235, 47,
                108, 25, 183, 118, 74, 185, 107, 168, 12, 158, 163, 116, 114, 29, 92, 198, 59, 98,
                15, 227, 36, 67, 32, 231, 140, 166, 195, 102, 58, 52, 244, 208, 1, 230, 138, 150,
                22, 154, 137, 142, 219, 129, 78, 128, 157, 4, 69, 21, 82, 209, 144, 196, 217, 149,
            ],
        };

        test_deletion(delete_input);
    }

    fn test_deletion(delete_input: DeleteInputs) {
        let mut table = setup_test_table();

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

            assert_eq!(output, expected_output)
        }

        clean_test();
    }

    #[test]
    fn quickcheck_insert_delete_and_select() {
        // Change the Gen::new(size) to have quickcheck
        // generate larger size vector.
        let gen = Gen::new(100);

        QuickCheck::new()
            .gen(gen)
            .quickcheck(insert_delete_and_select_prop as fn(DeleteInputs) -> bool);
    }

    fn insert_delete_and_select_prop(delete_input: DeleteInputs) -> bool {
        let mut table = setup_test_table();

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

        clean_test();
        true
    }

    fn setup_test_table() -> Table {
        return Table::new(format!("test-{:?}.db", std::thread::current().id()), 8);
    }

    fn clean_test() {
        let _ = std::fs::remove_file(format!("test-{:?}.db", std::thread::current().id()));
    }
}
