from ..enums import Filter_Type

class FilterObject:
    def __init__(self,value,_type:Filter_Type):

        self.value = value
        self.classification = _type


