from ...connectors import BaseDBConnector
from ...enums import Prompt_Type
from ...db_schema import Table, Database

from typing import NamedTuple
from .prompts import Intro_Prompt

class Example(NamedTuple):
    db: list[list[Table]]
    question: list[str]
    answer: list[str]



class PipelineSqlGen:

    def __init__(self, _connection: BaseDBConnector):
        self.connection = _connection
        self.db = _connection.scan_db()
        self.custom_prompt = None

    def reload_database(self) -> None:
        filter_list = self.db.filter_list
        filter_active = self.db.filter_active

        new_db = self.connection.scan_db()
        new_db.filter_list = filter_list
        new_db.filter_active = filter_active
        new_db.determine_filtered_elements()
        self.db = new_db

    def apply_table_name_filter(self, _table_names: list[str]) -> list[str]:
        self.db.apply_name_filter(_table_names)
        return self.db.get_filtered_tables()

    def apply_table_regex_filter(self, _regex: str) -> list[str]:
        self.db.apply_regex_filter(_regex)
        return self.db.get_filtered_tables()

    def set_custom_examples(self, examples: Example):
        custom_prompt = ""
        for i in range(len(examples["db"])):
            custom_prompt += f'Question: {examples["question"][i]} \n'

            new_db = Database("example")
            new_db.register_tables(examples["db"][i])
            custom_prompt += f'Database:\n{new_db.return_code_repr_schema()}\n'
            custom_prompt += f'Answer: {examples["answer"][i]}\n\n'
        self.custom_prompt = custom_prompt
        print("GENERATED CUSTOM FEW SHOT PROMPT:\n", self.custom_prompt)

    def overwrite_custom_examples(self,provided_examples:str):
        self.custom_prompt = provided_examples


    def return_db_prompt(self):
        #TODO allow different db representations like text custom
        return self.db.return_code_repr_schema()

    def generate_prompt(self, question:str,prompting_mode: Prompt_Type):
        prompt = ""
        prompt += Intro_Prompt
        if prompting_mode == Prompt_Type.CUSTOM:
            if self.custom_prompt is None:
                raise ValueError(f'If you want to use custom examples you need to set a custom prompt first with'
                                 f'set_examples()')
            prompt += self.custom_prompt
        elif prompting_mode == Prompt_Type.DYNAMIC:
            raise NotImplementedError("Not yet implemented")

        elif prompting_mode == Prompt_Type.FEW_SHOT:
            raise NotImplementedError("Not yet implemented")

        elif prompting_mode == Prompt_Type.ZERO_SHOT:
            pass

        prompt += f'Question: {question} \n'
        prompt += f'Database:\n{self.return_db_prompt()}\n'
        prompt += f'Answer: SELECT '

        return prompt



