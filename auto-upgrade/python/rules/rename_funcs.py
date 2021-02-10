from bowler import Query
import sys
import os

no_prompt= "NO_PROMPT" in os.environ
silent = "SILENT" in os.environ

migration_pairs = [("toDegrees", "degrees"), ("toRadians", "radians")]
def make_func_rename_query(old_name, new_name):
    print("Making query to rewrite " + old_name + " to " + new_name)
    return (
        Query(sys.argv[1])
        .select_function(old_name)
        .rename(new_name)
        .execute(interactive=(not no_prompt), write=False, silent=silent)
    )

for migration_pair in migration_pairs:
    make_func_rename_query(*migration_pair)
