import duckdb

class BFSType:
    def __init__(self, implementation: str, commit: str, active_mask: bool):
        self.implementation = implementation
        self.commit = commit
        self.active_mask = active_mask

    def __repr__(self):
        return (f"BFSType(implementation='{self.implementation}', "
                f"commit='{self.commit}', active_mask={self.active_mask})")

def read_file(conn, experiment):
    result = conn.sql(f"""
                create table '{experiment.commit}' as 
                WITH params_timing as (
                    SELECT      str_split(str_split(str_split(name, '/')[-1], '/')[1], '_') as params,
                                avg(timing) as mean_time
                    FROM        read_csv('sed "s/  */,/g" /Users/dljtw/git/duckpgq/output_{experiment.commit}.timing |')
                    GROUP BY    name)    
                select 
                        params[2][3:] as sf,
                        params[3][4:] as src,
                        params[4][2:] as threads,
                        mean_time,
                        'mac' as machine,
                        partition_type: 'multi-phase-hl',
                        bfs_type: '{experiment.implementation}',
                        commit: '{experiment.commit}',
                        active_mask: {experiment.active_mask}
                FROM params_timing
        """)
    print(conn.sql(f"""SELECT * FROM '{experiment.commit}'"""))
def main():
    experiments = [
        BFSType("single_directional", "6addfbf", True),
        BFSType("single_directional", "4ee1534", False),
        BFSType("bottom_up", "df230fc", True),
        BFSType("bottom_up", "c83b6f8", False),
        BFSType("bidirectional", "79a018e", True),
        BFSType("bidirectional", "f3eaff4", False),
    ]

    conn = duckdb.connect()
    conn.install_extension("shellfs", repository="community")
    conn.load_extension("shellfs")
    for experiment in experiments:
        read_file(conn, experiment)
    union_statement = "COPY ("
    for i in range(len(experiments)):
        union_statement += f"FROM '{experiments[i].commit}' " if i == len(experiments) - 1 else f"FROM '{experiments[i].commit}' UNION ALL "
    union_statement += ") TO 'bfs-implementation-experiments.csv'"
    conn.sql(union_statement)



if __name__ == "__main__":
    main()