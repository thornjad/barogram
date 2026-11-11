import models._confidence as confidence
import similarity_tune


def test_every_candidate_vector_weights_every_feature():
    features = confidence._DEFAULT_FEATURES + confidence._TREND_FEATURES
    for name, weights in similarity_tune.CANDIDATE_WEIGHT_VECTORS.items():
        if weights is not None:
            assert set(features) <= set(weights), name


def test_target_labels_are_unique():
    labels = [label for label, _, _ in similarity_tune.TARGETS]
    assert len(labels) == len(set(labels)) == 6
