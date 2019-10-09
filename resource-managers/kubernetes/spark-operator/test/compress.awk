# Helper functions
function trim(s) {
	gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", s);
	return s;
}

function printRecordAndCount(record, count) {
	print record;
	if (count > 1) {
		printf("... repeated %d times\n", count)
	}
}

BEGIN {
	# Before processing, set the record separator to the ASCII record separator character \x1e
	RS = "\x1e";
}

# This action is executed for each record
{
	# Build our current var from the trimmed record
	current = trim($0);

	# Bump the count of times we have seen it
	seen[current]++;

	# Print the previous record and its count (if it is not identical to the current record)
	if (previous && previous != current) {
		printRecordAndCount(previous, seen[previous]);
	}

	# Store the current record as the previous record
	previous = current;
}

END {
	# After processing, print the last record and count if it is non-empty
	if (previous) {
		printRecordAndCount(previous, seen[previous]);
	}
}