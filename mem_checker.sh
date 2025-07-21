pid=$(pgrep unittest)  # Find process ID of unittest
while ps -p $pid > /dev/null; do
    ts=$(date +%s)  # Timestamp
    mem=$(ps -o rss= -p $pid)  # Resident memory in KB
    echo "$ts $mem" >> memory_log.txt
    sleep 1  # Adjust sampling rate as needed
done
