use bplustree::BPlustree;

fn main() {
    let mut bplustree = BPlustree::<u64, f64>::new("./testfloat.bplustree");
    bplustree.set(&1, &10.23f64).unwrap();
    bplustree.set(&2, &99.9f64).unwrap();
    
    println!("{}", bplustree.get(&1).unwrap()); //10.23
}
