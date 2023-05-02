"""Utility script to inspect the provided datasets for the hackathon."""

import argparse
from typing import List, Optional, Tuple

import pandas as pd
import sqlite3


# Utility function(s)
def get_table_names(database: str) -> List[str]:
    with sqlite3.connect(database) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name "
            "FROM sqlite_master "
            "WHERE type='table';"
        )
        table_names = [tup[0] for tup in cursor.fetchall()]
    return table_names

def get_index_names(database: str) -> List[Tuple[str, str]]:
    with sqlite3.connect(database) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, tbl_name "
            "FROM sqlite_master "
            "WHERE type='index';"
        )
        index_names = cursor.fetchall()
    return index_names

def get_column_names(database: str, table_name: str) -> List[str]:
    with sqlite3.connect(database) as conn:
        column_names = pd.read_sql_query(
            "SELECT * "
            f"FROM {table_name} "
            "LIMIT 1",
            conn,
        ).columns.values.tolist()
    return column_names

def get_number_of_events(
        database: str,
        truth_table_name: str,
        index_column: str,
        group_by: Optional[List[str]] = None
    ) -> pd.DataFrame:
    if group_by:
        query = (
            f"SELECT {', '.join(group_by)}, COUNT({index_column}) "
            f"FROM {truth_table_name} "
            f"GROUP BY {', '.join(group_by)}"
        )
    else:
        query = (
            f"SELECT COUNT({index_column}) "
            f"FROM {truth_table_name}"
        )

    with sqlite3.connect(database) as conn:
        number_of_events = pd.read_sql_query(query, conn)
    return number_of_events

def get_query_plan(
        database: str,
        pulse_table_name: str,
        index_column: str ,
        index: int = 1,
    ) -> str:
    with sqlite3.connect(database) as conn:
        query = (
            "EXPLAIN QUERY PLAN "
            "SELECT * "
            f"FROM {pulse_table_name} "
            f"WHERE {index_column}={index}"
        )
        query_plan = pd.read_sql(query, conn).loc[0, "detail"]

    return query_plan


# Main function definition(s)
def main(
        name: str = "Kaggle",
        database: str = "kaggle/first_4_batches.db",
        index_column: str = "event_id",
        truth_table_name: str = "meta_table",
        pulse_table_name: str = "pulse_table",
        count_group_by: Optional[List[str]] = None,
    ) -> None:

    print(f"{'-' * 40}\nInspecting \033[1m{name}\033[0m dataset:")

    # Table names
    table_names = get_table_names(database)
    print(f"\n * Available tables: {table_names}")

    # Index names
    index_names = get_index_names(database)
    print(f"\n * Available indexes: {index_names}")

    # Column names
    print("\n * Available columns:")
    for table_name in table_names:
        column_names = get_column_names(database, table_name)
        print(f"    > {table_name}: {column_names}")

    # Number of events
    number_of_events = get_number_of_events(
        database,
        truth_table_name,
        index_column,
        group_by=count_group_by,
    )
    print(f"\n * Number of events in dataset:\n{number_of_events}")

    # Query plan
    query_plan = get_query_plan(database, pulse_table_name, index_column)


# Main function call
if __name__ == "__main__":

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Inspect the provided datasets for the hackathon",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Inspect all datasets.",
    )
    parser.add_argument(
        "--kaggle",
        action="store_true",
        help="Inspect the Kaggle dataset.",
    )
    parser.add_argument(
        "--prometheus-orca",
        action="store_true",
        help="Inspect the Prometheus ORCA dataset.",
    )
    parser.add_argument(
        "--icecube-northern-tracks",
        action="store_true",
        help="Inspect the IceCube northern tracks dataset.",
    )
    parser.add_argument(
        "--icecube-oscnext",
        action="store_true",
        help="Inspect the IceCube OscNext dataset.",
    )
    parser.add_argument(
        "--icecube-upgrade",
        action="store_true",
        help="Inspect the IceCube Upgrade dataset.",
    )

    args = parser.parse_args()

    # Generic
    if args.all or args.kaggle:
        main(
            name="Kaggle",
            database="generic/kaggle.db",
            index_column="event_id",
            truth_table_name="meta_table",
            pulse_table_name="pulse_table",
        )

    if args.all or args.prometheus_orca:
        main(
            name="Prometheus",
            database="generic/prometheus-orca.db",
            index_column="event_no",
            truth_table_name="mc_truth",
            pulse_table_name="total",
            count_group_by=["injection_type", "injection_interaction_type"],
        )

    # IceCube-specific
    if args.all or args.icecube_northern_tracks:
        main(
            name="IceCube Northern Tracks",
            database="icecube/northern_tracks.db",
            index_column="event_no",
            truth_table_name="truth",
            pulse_table_name="HVInIcePulses",
            count_group_by=["pid"],
        )

    if args.all or args.icecube_oscnext:
        main(
            name="IceCube OscNext Level-7",
            database="icecube/oscnext.db",
            index_column="event_no",
            truth_table_name="truth",
            pulse_table_name="SRTTWOfflinePulsesDC",
            count_group_by=["pid"],
        )

    if args.all or args.icecube_upgrade:
        main(
            name="IceCube Upgrade",
            database="icecube/upgrade.db",
            index_column="event_no",
            truth_table_name="truth",
            pulse_table_name="SplitInIcePulses_dynedge_v2_Pulses",
            count_group_by=["pid"],
        )

