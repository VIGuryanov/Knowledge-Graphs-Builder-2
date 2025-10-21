from enum import Enum
import re

class TripleItemType(Enum):
    iri = 1,
    int = 2,
    str = 3,
    var = 4


class TripleItem:
    def __init__(self, type: TripleItemType, value):
        self.type = type
        self.value = value

    @staticmethod
    def create_from_dict(type_value: dict):
        if 'type' in type_value and 'value' in type_value:
            if (type(type_value['type']) is str and (type_value['type'].lower() == 'uri' or type_value['type'].lower() == 'bnode')) or (type_value['type'] == TripleItemType.iri):
                return TripleItemIri(type_value['value'])
            elif (type(type_value['type']) is str and (type_value['type'].lower() == 'literal' or 
                                                       type_value['type'].lower() == 'typed-literal' and type_value['datatype'] == 'http://www.w3.org/2001/XMLSchema#string')) or (type_value['type'] == TripleItemType.str):
                return TripleItemStr(type_value['value'])
            else:
                #TODO int
                raise Exception(f"invalid type_value {type_value['type']}")
        else:
            raise Exception(f"invalid type_value {type_value}")
    
class TripleItemIri(TripleItem):
    def __init__(self, value):
        super().__init__(TripleItemType.iri, value)
    
    def __str__(self):
        return f"<{self.value}>"

class TripleItemInt(TripleItem):
    def __init__(self, value):
        super().__init__(TripleItemType.int, value)
    
    def __str__(self):
        return f"{self.value}"

class TripleItemStr(TripleItem):
    def __init__(self, value):
        value = re.sub(r'[^A-Za-zА-Яа-я0-9 ]', '', value)
        super().__init__(TripleItemType.str, value)
    
    def __str__(self):
        return f'"{self.value}"'

class TripleItemVariable(TripleItem):
    def __init__(self, value):
        super().__init__(TripleItemType.var, value)
    
    def __str__(self):
        return f'?{self.value}'

class Triple:
    def __init__(self, subject:TripleItemIri|TripleItemVariable, predicate:TripleItemIri|TripleItemVariable, object:TripleItem):
        if subject.type != TripleItemType.iri and subject.type != TripleItemType.var:
            raise Exception("Subject must be iri or variable type")
        if predicate.type != TripleItemType.iri and predicate.type != TripleItemType.var:
            raise Exception("Predicate must be iri or variable type")
        
        self.subject = subject
        self.predicate = predicate
        self.object = object