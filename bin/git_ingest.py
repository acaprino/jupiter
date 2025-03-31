# Synchronous usage
from gitingest import ingest

summary, tree, content = ingest(source="..", output="out.txt", exclude_patterns={"*.csv", "*.log", "*.log.*"})
