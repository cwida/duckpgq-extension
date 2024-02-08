import kuzu
import pandas as pd

db = kuzu.Database('./test')
conn = kuzu.Connection(db)

# Drop the table if it exists:
try:
    conn.execute("DROP TABLE knows")
except:
    pass
try:
    conn.execute("DROP TABLE Person")
except:
    pass

# Define the schema:
conn.execute("CREATE NODE TABLE Person (creationDate TIMESTAMP, id INT64, firstName STRING, lastName STRING, gender STRING, birthday DATE, locationIP STRING, browserUsed STRING, LocationCityId INT64, speaks STRING, email STRING, PRIMARY KEY (id))")
conn.execute("CREATE REL TABLE knows (FROM Person TO Person)")

# Load the data:
conn.execute("Copy Person FROM './test/person.csv'")
conn.execute("Copy knows FROM './test/person_knows_person.csv'")

# Calculate the shortest path between two people with bounded distance:
MIN_DISTANCE = 0
MAX_DISTANCE = 30
results = pd.DataFrame()
for low in range(MIN_DISTANCE, MAX_DISTANCE + 1):
    for high in range(low, MAX_DISTANCE + 1):
        result = conn.execute("MATCH (a:Person)-[e:knows*%d..%d]->(b:Person) RETURN a.id, b.id, length(e) AS distance ORDER BY distance ASC" % (low, high)).get_as_df()
        result = result.drop_duplicates(subset=['a.id', 'b.id'], keep='first')
        result['min_distance'] = low
        result['max_distance'] = high
        results = pd.concat([results, result], ignore_index=True)

results.to_csv('./test/shortest_length_kuzu.csv', index=False)