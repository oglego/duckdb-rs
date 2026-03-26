// Basic vscalar example - registering a custom scalar function
use duckdb::{
    Connection, Result,
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    types::DuckString,
};
use libduckdb_sys::duckdb_string_t;

struct ToUpper {}

impl VScalar for ToUpper {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let out = output.flat_vector();
        let values = input.flat_vector(0);
        let values = values.as_slice_with_len::<duckdb_string_t>(input.len());

        for value in values.iter().take(input.len()) {
            let mut owned = *value;
            let s = DuckString::new(&mut owned).as_str();
            out.insert(0, s.to_uppercase().as_str());
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.register_scalar_function::<ToUpper>("to_upper")?;

    let mut stmt = conn.prepare("SELECT to_upper(name) FROM (VALUES ('test')) t(name)")?;
    let person_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;

    for result in person_iter {
        println!("Result: {}", result.unwrap());
    }

    Ok(())
}