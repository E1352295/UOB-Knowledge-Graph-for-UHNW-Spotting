@echo off
REM Set Neo4j connection environment variables
set "NEO4J_URI=neo4j+s://d8d4e86b.databases.neo4j.io"
set "NEO4J_USER=neo4j"
set "NEO4J_PASSWORD=IVVi_p1Rl2ca-O5g5ULkd5KHtg2uSXkLaj1So_oHL4Q"

REM Call Python script with the same arguments
python load_graph.py ^
  --neo4j_export "../SGX Annual Reports/Case Study/Venture Corporation Limited/neo4j_query_table_data_2025-6-26.json" ^
  --wikidata_json "../WikiData/data.json" ^
  --mas_personnel_csv "../MAS/MAS_Personnel_merged.csv"

REM Optional: pause to see output
pause
