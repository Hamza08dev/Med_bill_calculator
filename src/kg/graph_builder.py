import os
import pandas as pd
import glob
from neo4j import GraphDatabase

def setup_knowledge_graph(driver, data_dir):
    print("Setting up the Knowledge Graph for all schedules...")
    with driver.session(database="neo4j") as session:
        print("  - Wiping existing data...")
        session.run("MATCH (n) DETACH DELETE n")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Procedure) REQUIRE p.code IS UNIQUE;")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Region) REQUIRE r.name IS UNIQUE;")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sector) REQUIRE s.name IS UNIQUE;")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (sch:Schedule) REQUIRE sch.name IS UNIQUE;")
        session.run("UNWIND ['Region I', 'Region II', 'Region III', 'Region IV'] AS region_name MERGE (r:Region {name: region_name})")

        print("  - Loading Conversion Factors...")
        cf_df = pd.read_csv(os.path.join(data_dir, "Section_conversion.csv"))
        cf_df.columns = [c.strip().lower().replace(' ', '_') for c in cf_df.columns]
        for _, row in cf_df.iterrows():
            session.run("MERGE (sch:Schedule {name: $schedule})", {"schedule": row['schedule']})
            session.run("MERGE (s:Sector {name: $section})", {"section": row['section']})
            session.run("""
                MATCH (r:Region {name: $region}), (s:Sector {name: $section})
                MERGE (r)-[rel:HAS_CONVERSION_FACTOR {schedule: $schedule}]->(s)
                SET rel.value = toFloat($conv_factor)
            """, row.to_dict())

        all_csvs = glob.glob(os.path.join(data_dir, "*.csv"))
        procedure_files = [f for f in all_csvs if "Section_conversion" not in f and "Zip_regions" not in f]
        
        for filepath in procedure_files:
            filename = os.path.basename(filepath)
            try:
                # Get both sector and schedule from the filename
                sector_name_raw, schedule_name_raw = filename.replace('.csv', '').split('_')
            except ValueError:
                print(f"  - Skipping file with incorrect format: {filename}")
                continue
            
            if sector_name_raw.lower() == 'e-m':
                sector_name = 'E/M'
            else:
                sector_name = sector_name_raw.replace('-', ' ').title()
            
            schedule_name = schedule_name_raw.lower()

            print(f"  - Loading procedures from {filename} for Schedule '{schedule_name}' and Sector '{sector_name}'...")
            proc_df = pd.read_csv(filepath)
            proc_df.columns = [str(col).lower().replace(' ', '_').replace('/', '_') for col in proc_df.columns]
            
            if 'code' not in proc_df.columns or 'schedule' not in proc_df.columns: continue
            
            proc_df.dropna(subset=['code', 'schedule'], inplace=True)
            proc_df['code'] = proc_df['code'].astype(str).str.replace(r'\.0$', '', regex=True)
            if 'pc_tc_split' not in proc_df.columns: proc_df['pc_tc_split'] = ''
            proc_df = proc_df.fillna('')
            procedures_data = proc_df.to_dict('records')
            
            session.run("""
                UNWIND $procedures AS row
                MERGE (p:Procedure {code: row.code})
                SET p.rvu = toFloat(row.relative_value), 
                    p.pc_tc_split = CASE WHEN row.pc_tc_split = '' THEN null ELSE row.pc_tc_split END
                WITH p, row
                MATCH (s:Sector {name: $sector_name})
                MATCH (sch:Schedule {name: row.schedule})
                MERGE (p)-[:BELONGS_TO]->(s)
                MERGE (p)-[:IN_SCHEDULE]->(sch)
            """, {"procedures": procedures_data, "sector_name": sector_name})
    print("Knowledge Graph setup complete.")