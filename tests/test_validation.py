from lakehouse.validation.validation_runner import validate_records


def test_validate_records_scores_required_keys():
    result = validate_records([{"id": "1"}, {"name": "missing"}], ["id"])
    assert result["score"] == 0.5
    assert result["failures"][0]["field"] == "id"
