use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use libduckdb_sys::duckdb_string_t;

/// A simple custom scalar function that converts a string to uppercase.
struct ToUpper;

impl VScalar for ToUpper {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let out = output.flat_vector();
        let values = input.flat_vector(0);
        
        // Convert the input vector to a slice of DuckDB strings
        let values_slice = values.as_slice_with_len::<duckdb_string_t>(input.len());

        for value in values_slice.iter().take(input.len()) {
            let mut owned = *value;
            // Access the string data safely through the DuckString wrapper
            let s = DuckString::new(&mut owned).as_str();
            
            // Perform the logic and insert into the output vector
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

    // Register the custom scalar function with the name "my_to_upper"
    conn.register_scalar_function::<ToUpper>("my_to_upper")?;

    // Use the function in a SQL query
    let mut stmt = conn.prepare("SELECT my_to_upper('hello duckdb')")?;
    let mut rows = stmt.query([])?;

    if let Some(row) = rows.next()? {
        let result: String = row.get(0)?;
        println!("Result: {}", result); // Output: HELLO DUCKDB
    }

    Ok(())
}