  export NEO4J_URI='neo4j+s://d8d4e86b.databases.neo4j.io'
  export NEO4J_USER='neo4j'
  export NEO4J_PASSWORD='IVVi_p1Rl2ca-O5g5ULkd5KHtg2uSXkLaj1So_oHL4Q'
  python load_graph.py \
      --neo4j_export '../SGX Annual Reports/Case Study/Venture Corporation Limited/neo4j_query_table_data_2025-6-26.json' \
      --wikidata_json '../WikiData/data.json' \
      --mas_personnel_csv '../MAS/MAS_Personnel_merged.csv'