from ..enums import Filter_Type


class EmbeddingContainer:

    def __init__(self, _embedding, _cosine_dist_threshold):
        self.embedding = _embedding
        self.threshold = _cosine_dist_threshold


class FilterObject:
    def __init__(self, value, _type: Filter_Type, threshold=None):
        """
        Filter object representing an active filter
        @param value: can be either list[str], str, or EmbeddingContainer
        @param _type:
        """
        if _type == Filter_Type.EMBEDDING:
            self.value = EmbeddingContainer(value,threshold)
        else:
            self.value = value
        self.classification = _type
