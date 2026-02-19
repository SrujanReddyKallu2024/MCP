import json

from pyspark.sql import DataFrame, SparkSession
import pytest

from src.spark.stck01_generate_data import (
    Stats,
    save_json_to_s3,
    format_graph,
    apply_rank,
    join_with_lookup,
    calculate_num_partitions,
    logic_main,
)


class TestStats:
    """Tests for Stats class"""

    def test_init(self):
        """Test Stats initialization"""
        stats = Stats()
        assert stats._stats == []

    def test_add_stats_success(self):
        """Test adding stats with success status"""
        stats = Stats()
        stats.add_stats("Test Milestone", "success", "Test message")

        assert len(stats._stats) == 1
        assert stats._stats[0]["Milestone"] == "Test Milestone"
        assert stats._stats[0]["Message"] == "Test message"
        assert stats._stats[0]["Status"] == "✅"

    def test_add_stats_failed(self):
        """Test adding stats with failed status"""
        stats = Stats()
        stats.add_stats("Test Milestone", "failed", "Error occurred")

        assert len(stats._stats) == 1
        assert stats._stats[0]["Status"] == "❌"

    def test_add_stats_default_status(self):
        """Test adding stats with default status"""
        stats = Stats()
        stats.add_stats("Test Milestone")

        assert stats._stats[0]["Status"] == "✅"

    def test_get_stats(self):
        """Test getting stats"""
        stats = Stats()
        stats.add_stats("Milestone 1", "success", "Message 1")
        stats.add_stats("Milestone 2", "failed", "Message 2")

        result = stats.get_stats()
        assert len(result) == 2
        assert result[0]["Milestone"] == "Milestone 1"
        assert result[1]["Milestone"] == "Milestone 2"


def test_save_json_to_s3(s3_mock, s3_bucket):
    """Test saving JSON to S3"""
    test_data = {"key": "value", "number": 42}
    test_path = f"s3://{s3_bucket}/test-key.json"

    save_json_to_s3(s3_mock, test_data, test_path)

    response = s3_mock.get_object(Bucket=s3_bucket, Key="test-key.json")
    content = response["Body"].read().decode("utf-8")
    loaded_data = json.loads(content)

    assert loaded_data == test_data


class TestFormatGraph:
    """Tests for format_graph function"""

    def test_format_graph_with_cutoff(self, spark: SparkSession):
        """Test format_graph with date cutoff"""
        data = [
            (1, "chv,de", "2024-01-10"),
            (2, "uc", "2024-01-05"),
            (3, "chv", "2023-12-20"),
        ]
        schema = ["match_id", "source", "date"]
        df = spark.createDataFrame(data, schema)

        result = format_graph(df, "date", 10)
        result_collect = [row.asDict() for row in result.collect()]
        print(result_collect)

        expected_result = [
            {
                "match_id": 1,
                "source": "chv,de",
                "date": "2024-01-10",
                "source_array": ["chv", "de"],
                "source_count": 2,
                "chv": 1,
                "de": 1,
                "uc": 0,
            },
            {
                "match_id": 2,
                "source": "uc",
                "date": "2024-01-05",
                "source_array": ["uc"],
                "source_count": 1,
                "chv": 0,
                "de": 0,
                "uc": 1,
            },
        ]

        assert result_collect == expected_result

    def test_format_graph_no_cutoff(self, spark: SparkSession):
        """Test format_graph without date cutoff"""
        data = [
            (1, "chv,de", "2024-01-10"),
            (2, "uc", "2023-12-01"),
        ]
        schema = ["match_id", "source", "date"]
        df = spark.createDataFrame(data, schema)
        result = format_graph(df, "date", -1)
        result_collect = [row.asDict() for row in result.collect()]
        print(result_collect)

        expected_result = [
            {
                "match_id": 1,
                "source": "chv,de",
                "date": "2024-01-10",
                "source_array": ["chv", "de"],
                "source_count": 2,
                "chv": 1,
                "de": 1,
                "uc": 0,
            },
            {
                "match_id": 2,
                "source": "uc",
                "date": "2023-12-01",
                "source_array": ["uc"],
                "source_count": 1,
                "chv": 0,
                "de": 0,
                "uc": 1,
            },
        ]

        assert result_collect == expected_result


class TestApplyRank:
    """Tests for apply_rank function"""

    def test_apply_rank_ip(self, spark: SparkSession):
        """Test apply_rank for IP identifier

        Args:
            spark (SparkSession): Spark session fixture
        """
        data = [
            (1, "192.168.1.1", "2024-01-01"),
            (1, "192.168.1.3", "2024-01-02"),
            (1, "192.168.1.2", "2024-01-03"),
            # test filter out more than 5 entries for same match_id
        ] + [(2, f"192.168.1.{x + 1}", "2024-01-01") for x in range(6)]
        schema = ["match_id", "ip", "date"]
        df = spark.createDataFrame(data, schema)

        result = apply_rank(df, "ip", "date")
        result_collect = [row.asDict() for row in result.collect()]
        print(result_collect)

        result_rankings = [
            {x: y for x, y in row.items() if x in ["match_id", "ip", "rnum"]}
            for row in result_collect
        ]
        result_rankings.sort(key=lambda x: (x["match_id"], x["rnum"]))
        expected_result = [
            {"match_id": 1, "ip": "192.168.1.3", "rnum": 1},
            {"match_id": 1, "ip": "192.168.1.2", "rnum": 2},
            {"match_id": 1, "ip": "192.168.1.1", "rnum": 3},
        ] + [{"match_id": 2, "ip": f"192.168.1.{6 - x}", "rnum": x + 1} for x in range(5)]
        expected_result.sort(key=lambda x: (x["match_id"], x["rnum"]))

        assert result_rankings == expected_result

    def test_apply_rank_non_ip(self, spark: SparkSession):
        """Test apply_rank for non-IP identifier

        Args:
            spark (SparkSession): Spark session fixture
        """
        data = [
            (1, "id1", 2, 3, 1, 1, 1, "2024-01-01"),
            (1, "id2", 5, 2, 1, 0, 0, "2024-01-01"),
            (1, "id3", 3, 1, 0, 1, 0, "2024-01-01"),
            # test higher source_count
            (2, "id1", 3, 3, 1, 1, 1, "2024-01-01"),
            (2, "id2", 3, 2, 1, 0, 1, "2024-01-01"),
            # test de higher priority than chv
            (3, "id1", 3, 1, 0, 1, 0, "2024-01-01"),
            (3, "id2", 3, 1, 1, 0, 0, "2024-01-01"),
            # test de higher priority than uc
            (4, "id1", 3, 1, 0, 0, 1, "2024-01-01"),
            (4, "id2", 3, 1, 1, 0, 0, "2024-01-01"),
            # test chv higher priority than uc
            (5, "id1", 3, 1, 0, 0, 1, "2024-01-01"),
            (5, "id2", 3, 1, 0, 1, 0, "2024-01-01"),
            # test date tie-breaker
            (6, "id1", 3, 1, 0, 1, 0, "2024-01-02"),
            (6, "id2", 3, 1, 0, 1, 0, "2024-01-01"),
            # test filter out more than 10 entries for same match_id
        ] + [(7, f"id{x + 1}", x + 1, 1, 1, 0, 0, "2024-01-01") for x in range(11)]
        schema = ["match_id", "uid", "diff", "source_count", "de", "chv", "uc", "date"]
        df = spark.createDataFrame(data, schema)

        result = apply_rank(df, "uid", "date")
        result_collect = [row.asDict() for row in result.collect()]
        print(result_collect)

        result_rankings = [
            {x: y for x, y in row.items() if x in ["match_id", "uid", "rnum"]}
            for row in result_collect
        ]
        result_rankings.sort(key=lambda x: (x["match_id"], x["rnum"]))
        expected_result = [
            {"match_id": 1, "uid": "id1", "rnum": 1},
            {"match_id": 1, "uid": "id3", "rnum": 2},
            {"match_id": 1, "uid": "id2", "rnum": 3},
            {"match_id": 2, "uid": "id1", "rnum": 1},
            {"match_id": 2, "uid": "id2", "rnum": 2},
            {"match_id": 3, "uid": "id2", "rnum": 1},
            {"match_id": 3, "uid": "id1", "rnum": 2},
            {"match_id": 4, "uid": "id2", "rnum": 1},
            {"match_id": 4, "uid": "id1", "rnum": 2},
            {"match_id": 5, "uid": "id2", "rnum": 1},
            {"match_id": 5, "uid": "id1", "rnum": 2},
            {"match_id": 6, "uid": "id1", "rnum": 1},
            {"match_id": 6, "uid": "id2", "rnum": 2},
        ] + [{"match_id": 7, "uid": f"id{x + 1}", "rnum": x + 1} for x in range(10)]
        expected_result.sort(key=lambda x: (x["match_id"], x["rnum"]))

        assert result_rankings == expected_result


def test_join_with_lookup(spark: SparkSession):
    """Test join_with_lookup"""
    id_data = [
        (1, "uid1", "2024-01-01", "extra1"),
        (2, "uid2", "2024-01-02", "extra2"),
        (3, "uid3", "2024-01-03", "extra3"),
    ]
    id_schema = ["match_id", "uid", "date", "extra_col"]
    lookup_data = [
        (1, "hhk1"),
        (2, "hhk2"),
    ]
    lookup_schema = ["match_id", "match_id_cid00025"]
    id_df = spark.createDataFrame(id_data, id_schema)
    lookup_df = spark.createDataFrame(lookup_data, lookup_schema)

    result = join_with_lookup(id_df, lookup_df, ["hhk", "uid", "date"])
    result_collect = [row.asDict() for row in result.collect()]
    print(result_collect)

    expected_result = [
        {"hhk": "hhk1", "uid": "uid1", "date": "2024-01-01"},
        {"hhk": "hhk2", "uid": "uid2", "date": "2024-01-02"},
    ]

    assert result_collect == expected_result


class TestCalculateNumPartitions:
    """Tests for calculate_num_partitions function"""

    def test_calculate_num_partitions_small(self):
        """Test partition calculation for small DataFrame"""
        result = calculate_num_partitions(1000, ["col1", "col2"])
        assert result == 1

    def test_calculate_num_partitions_large(self):
        """Test partition calculation for large DataFrame"""
        result = calculate_num_partitions(1_000_000_000, ["col1", "col2", "col3"])
        assert result >= 100

    def test_calculate_num_partitions_zero_rows(self):
        """Test partition calculation with zero rows"""
        result = calculate_num_partitions(0, ["col1"])
        assert result == 1


class TestLogicMain:
    """Tests for logic_main function"""

    def test_logic_main_ip(self, spark: SparkSession):
        """Test logic_main with IP identifier"""
        id_data = [
            (1, "192.168.1.1", "2024-01-03", "2024-01-12", 0.7, "de", 3, "extra1"),
            (2, "192.168.1.1", "2024-01-05", "2024-01-01", 0.9, "chv", 1, "extra2"),
            (2, "192.168.1.3", "2024-01-05", "2024-01-02", 0.8, "chv", 2, "extra3"),
            (2, "192.168.1.2", "2024-01-05", "2024-01-03", 0.7, "chv", 3, "extra4"),
            # test no matching lookup
            (99, "192.168.1.1", "2024-01-05", "2024-01-01", 0.6, "chv", 4, "extra5"),
            # test filter out more than 5 entries for same match_id
        ] + [
            (4, f"192.168.1.{x + 1}", "2024-01-05", "2024-01-01", 0.6, "chv", 4, f"extra{x + 6}")
            for x in range(6)
        ]
        id_schema = [
            "match_id",
            "ip",
            "first_seen",
            "last_seen",
            "conf_score",
            "source",
            "diff",
            "extra_col",
        ]
        lookup_data = [(1, "hhk1"), (2, "hhk2"), (4, "hhk4")]
        lookup_schema = ["match_id", "match_id_cid00025"]
        id_df = spark.createDataFrame(id_data, id_schema)
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        stats = Stats()

        result = logic_main(lookup_df, id_df, "ip", stats)
        result_collect = [row.asDict() for row in result.collect()]
        stat_list = stats.get_stats()
        print(result_collect)
        print(stat_list)

        expected_result = [
            {
                "hhk": "hhk1",
                "ip": "192.168.1.1",
                "first_seen": "2024-01-03",
                "last_seen": "2024-01-12",
                "conf_score": 0.7,
            },
            {
                "hhk": "hhk2",
                "ip": "192.168.1.1",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.9,
            },
            {
                "hhk": "hhk2",
                "ip": "192.168.1.2",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-03",
                "conf_score": 0.7,
            },
            {
                "hhk": "hhk2",
                "ip": "192.168.1.3",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-02",
                "conf_score": 0.8,
            },
            {
                "hhk": "hhk4",
                "ip": "192.168.1.2",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.6,
            },
            {
                "hhk": "hhk4",
                "ip": "192.168.1.3",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.6,
            },
            {
                "hhk": "hhk4",
                "ip": "192.168.1.4",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.6,
            },
            {
                "hhk": "hhk4",
                "ip": "192.168.1.5",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.6,
            },
            {
                "hhk": "hhk4",
                "ip": "192.168.1.6",
                "first_seen": "2024-01-05",
                "last_seen": "2024-01-01",
                "conf_score": 0.6,
            },
        ]

        expected_stats = [
            {
                "Milestone": "IP Data Load",
                "Message": "Latest ip data loaded with 11 records, date column: last_seen, "
                "id cutoff: 30 days",
                "Status": "✅",
            },
            {
                "Milestone": "IP Data Format",
                "Message": "Formatted ip data, updated count: 10",
                "Status": "✅",
            },
            {
                "Milestone": "IP Data with MatchId",
                "Message": "Joined ip data with lookup, updated count: 9",
                "Status": "✅",
            },
        ]

        assert result_collect == expected_result
        assert all("Time" in stats for stats in stat_list)
        stat_list_wo_time = [
            {k: v for k, v in stats.items() if k != "Time"} for stats in stat_list
        ]
        assert stat_list_wo_time == expected_stats

    def test_logic_main_maids(self, spark: SparkSession):
        """Test logic_main with maids identifier"""
        id_data = [
            (1, "maid1", "2024-01-12", "de", 3),
            # test higher diff
            (2, "maid1", "2024-01-10", "chv", 1),
            (2, "maid2", "2024-01-11", "chv", 2),
            # test higher source_count
            (3, "maid1", "2024-01-10", "chv", 2),
            (3, "maid2", "2024-01-11", "chv", 2),
            (3, "maid2", "2024-01-11", "de", 2),
            # test de higher priority than chv
            (4, "maid1", "2024-01-13", "chv", 4),
            (4, "maid2", "2024-01-13", "de", 4),
            # test chv higher priority than uc
            (5, "maid1", "2024-01-13", "chv", 4),
            (5, "maid2", "2024-01-13", "uc", 4),
            # test de higher priority than uc
            (6, "maid1", "2024-01-13", "de", 4),
            (6, "maid2", "2024-01-13", "uc", 4),
            # test date cutoff
            (7, "maid1", "2023-12-10", "chv", 5),
            # test no matching lookup
            (99, "maid1", "2024-01-14", "chv", 5),
            # test filter out more than 10 entries for same match_id
        ] + [(8, f"maid{x + 1}", "2024-01-01", "chv", x + 1) for x in range(11)]
        id_schema = ["match_id", "maids", "date", "source", "diff"]
        lookup_data = [
            (1, "hhk1"),
            (2, "hhk2"),
            (3, "hhk3"),
            (4, "hhk4"),
            (5, "hhk5"),
            (6, "hhk6"),
            (7, "hhk7"),
            (8, "hhk8"),
        ]
        lookup_schema = ["match_id", "match_id_cid00025"]
        id_df = spark.createDataFrame(id_data, id_schema)
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        stats = Stats()

        result = logic_main(lookup_df, id_df, "maids", stats)
        result_collect = [row.asDict() for row in result.collect()]
        stat_list = stats.get_stats()
        print(result_collect)
        print(stat_list)

        expected_result = [
            {"hhk": "hhk1", "maids": "maid1", "date": "2024-01-12"},
            {"hhk": "hhk2", "maids": "maid2", "date": "2024-01-11"},
            {"hhk": "hhk2", "maids": "maid1", "date": "2024-01-10"},
            {"hhk": "hhk3", "maids": "maid1", "date": "2024-01-10"},
            {"hhk": "hhk3", "maids": "maid2", "date": "2024-01-11"},
            {"hhk": "hhk4", "maids": "maid1", "date": "2024-01-13"},
            {"hhk": "hhk4", "maids": "maid2", "date": "2024-01-13"},
            {"hhk": "hhk5", "maids": "maid2", "date": "2024-01-13"},
            {"hhk": "hhk5", "maids": "maid1", "date": "2024-01-13"},
            {"hhk": "hhk6", "maids": "maid2", "date": "2024-01-13"},
            {"hhk": "hhk6", "maids": "maid1", "date": "2024-01-13"},
            {"hhk": "hhk7", "maids": "maid1", "date": "2023-12-10"},
            {"hhk": "hhk8", "maids": "maid10", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid9", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid8", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid7", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid6", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid5", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid4", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid3", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid2", "date": "2024-01-01"},
            {"hhk": "hhk8", "maids": "maid1", "date": "2024-01-01"},
        ]

        expected_stats = [
            {
                "Milestone": "MAIDS Data Load",
                "Message": "Latest maids data loaded with 25 records, date column: date, "
                "id cutoff: -1 days",
                "Status": "✅",
            },
            {
                "Milestone": "MAIDS Data Format",
                "Message": "Formatted maids data, updated count: 24",
                "Status": "✅",
            },
            {
                "Milestone": "MAIDS Data with MatchId",
                "Message": "Joined maids data with lookup, updated count: 22",
                "Status": "✅",
            },
        ]

        assert result_collect == expected_result
        assert all("Time" in stats for stats in stat_list)
        stat_list_wo_time = [
            {k: v for k, v in stats.items() if k != "Time"} for stats in stat_list
        ]
        assert stat_list_wo_time == expected_stats

    def test_logic_main_uid(self, spark: SparkSession):
        """Test logic_main with uid identifier"""
        id_data = [
            (1, "uid1", "2024-01-12", "de", 3),
            # test higher diff
            (2, "uid1", "2024-01-10", "chv", 1),
            (2, "uid2", "2024-01-11", "chv", 2),
            # test higher source_count
            (3, "uid1", "2024-01-10", "chv", 2),
            (3, "uid2", "2024-01-11", "chv", 2),
            (3, "uid2", "2024-01-11", "de", 2),
            # test de higher priority than chv
            (4, "uid1", "2024-01-13", "chv", 4),
            (4, "uid2", "2024-01-13", "de", 4),
            # test chv higher priority than uc
            (5, "uid1", "2024-01-13", "chv", 4),
            (5, "uid2", "2024-01-13", "uc", 4),
            # test de higher priority than uc
            (6, "uid1", "2024-01-13", "de", 4),
            (6, "uid2", "2024-01-13", "uc", 4),
            # test date cutoff
            (7, "uid1", "2023-12-10", "chv", 5),
            # test no matching lookup
            (99, "uid1", "2024-01-14", "chv", 5),
            # test filter out more than 10 entries for same match_id
        ] + [(8, f"uid{x + 1}", "2024-01-01", "chv", x + 1) for x in range(11)]
        id_schema = ["match_id", "uid", "date", "source", "diff"]
        lookup_data = [
            (1, "hhk1"),
            (2, "hhk2"),
            (3, "hhk3"),
            (4, "hhk4"),
            (5, "hhk5"),
            (6, "hhk6"),
            (7, "hhk7"),
            (8, "hhk8"),
        ]
        lookup_schema = ["match_id", "match_id_cid00025"]
        id_df = spark.createDataFrame(id_data, id_schema)
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        stats = Stats()

        result = logic_main(lookup_df, id_df, "uid", stats)
        result_collect = [row.asDict() for row in result.collect()]
        stat_list = stats.get_stats()
        print(result_collect)
        print(stat_list)

        expected_result = [
            {"hhk": "hhk1", "uid": "uid1", "date": "2024-01-12"},
            {"hhk": "hhk2", "uid": "uid2", "date": "2024-01-11"},
            {"hhk": "hhk2", "uid": "uid1", "date": "2024-01-10"},
            {"hhk": "hhk3", "uid": "uid1", "date": "2024-01-10"},
            {"hhk": "hhk3", "uid": "uid2", "date": "2024-01-11"},
            {"hhk": "hhk4", "uid": "uid1", "date": "2024-01-13"},
            {"hhk": "hhk4", "uid": "uid2", "date": "2024-01-13"},
            {"hhk": "hhk5", "uid": "uid2", "date": "2024-01-13"},
            {"hhk": "hhk5", "uid": "uid1", "date": "2024-01-13"},
            {"hhk": "hhk6", "uid": "uid2", "date": "2024-01-13"},
            {"hhk": "hhk6", "uid": "uid1", "date": "2024-01-13"},
            {"hhk": "hhk7", "uid": "uid1", "date": "2023-12-10"},
            {"hhk": "hhk8", "uid": "uid10", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid9", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid8", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid7", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid6", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid5", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid4", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid3", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid2", "date": "2024-01-01"},
            {"hhk": "hhk8", "uid": "uid1", "date": "2024-01-01"},
        ]

        expected_stats = [
            {
                "Milestone": "UID Data Load",
                "Message": "Latest uid data loaded with 25 records, date column: date, "
                "id cutoff: -1 days",
                "Status": "✅",
            },
            {
                "Milestone": "UID Data Format",
                "Message": "Formatted uid data, updated count: 24",
                "Status": "✅",
            },
            {
                "Milestone": "UID Data with MatchId",
                "Message": "Joined uid data with lookup, updated count: 22",
                "Status": "✅",
            },
        ]

        assert result_collect == expected_result
        assert all("Time" in stats for stats in stat_list)
        stat_list_wo_time = [
            {k: v for k, v in stats.items() if k != "Time"} for stats in stat_list
        ]
        assert stat_list_wo_time == expected_stats

    def test_logic_main_hems(self, spark: SparkSession):
        """Test logic_main with hems identifier"""
        id_data = [
            (1, "hem1"),
            (2, "hem2"),
            (2, "hem3"),
            (2, "hem4"),
            # test no matching lookup
            (99, "hem1"),
        ]
        id_schema = [
            "match_id",
            "hems",
        ]
        lookup_data = [(1, "hhk1"), (2, "hhk2"), (4, "hhk4")]
        lookup_schema = ["match_id", "match_id_cid00025"]
        id_df = spark.createDataFrame(id_data, id_schema)
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        stats = Stats()

        result = logic_main(lookup_df, id_df, "hems", stats)
        result_collect = [row.asDict() for row in result.collect()]
        stat_list = stats.get_stats()
        print(result_collect)
        print(stat_list)

        expected_result = [
            {"hhk": "hhk1", "hems": "hem1"},
            {"hhk": "hhk2", "hems": "hem4"},
            {"hhk": "hhk2", "hems": "hem3"},
            {"hhk": "hhk2", "hems": "hem2"},
        ]
        expected_stats = [
            {
                "Milestone": "HEMS Data Load",
                "Message": "Latest hems data loaded with 5 records, date column: , "
                "id cutoff: -1 days",
                "Status": "✅",
            },
            {
                "Milestone": "HEMS Data Format",
                "Message": "Formatted hems data, updated count: 5",
                "Status": "✅",
            },
            {
                "Milestone": "HEMS Data with MatchId",
                "Message": "Joined hems data with lookup, updated count: 4",
                "Status": "✅",
            },
        ]

        assert result_collect == expected_result
        assert all("Time" in stats for stats in stat_list)
        stat_list_wo_time = [
            {k: v for k, v in stats.items() if k != "Time"} for stats in stat_list
        ]
        assert stat_list_wo_time == expected_stats

    def test_logic_main_mobile(self, spark: SparkSession):
        """Test logic_main with mobile identifier"""
        id_data = [
            (1, "mobile1"),
            (2, "mobile2"),
            (2, "mobile3"),
            (2, "mobile4"),
            # test no matching lookup
            (99, "mobile1"),
        ]
        id_schema = [
            "match_id",
            "hashed_mobile",
        ]
        lookup_data = [(1, "hhk1"), (2, "hhk2"), (4, "hhk4")]
        lookup_schema = ["match_id", "match_id_cid00025"]
        id_df = spark.createDataFrame(id_data, id_schema)
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        stats = Stats()

        result = logic_main(lookup_df, id_df, "mobile", stats)
        result_collect = [row.asDict() for row in result.collect()]
        stat_list = stats.get_stats()
        print(result_collect)
        print(stat_list)

        expected_result = [
            {"hhk": "hhk1", "hashed_mobile": "mobile1"},
            {"hhk": "hhk2", "hashed_mobile": "mobile4"},
            {"hhk": "hhk2", "hashed_mobile": "mobile3"},
            {"hhk": "hhk2", "hashed_mobile": "mobile2"},
        ]

        expected_stats = [
            {
                "Milestone": "MOBILE Data Load",
                "Message": "Latest mobile data loaded with 5 records, date column: date, "
                "id cutoff: -1 days",
                "Status": "✅",
            },
            {
                "Milestone": "MOBILE Data Format",
                "Message": "Formatted mobile data, updated count: 5",
                "Status": "✅",
            },
            {
                "Milestone": "MOBILE Data with MatchId",
                "Message": "Joined mobile data with lookup, updated count: 4",
                "Status": "✅",
            },
        ]

        assert result_collect == expected_result
        assert all("Time" in stats for stats in stat_list)
        stat_list_wo_time = [
            {k: v for k, v in stats.items() if k != "Time"} for stats in stat_list
        ]
        assert stat_list_wo_time == expected_stats
