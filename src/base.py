from abc import ABC, abstractmethod
import pandas as pd

class Validator():
    '''wrapper class to run multiple validators'''
    def __init__(self,validators=[]):
        self.validators = validators
        
class BaseColumnValidator(ABC):
    """Base class for column validator"""

    # all indices that did not pass the validation
    failed_indices = []

    # this is the dataframe that's being passed to the next validator
    validated_df = None

    @abstractmethod
    def get_failed_indices(self, df, field):
        """this function should return all failed indices"""
        pass

    def validate(self, df, field):
        self.field = field
        self.validated_df = self.add_errors(df, self.get_failed_indices(df, field))

    def is_column_exist(self, df, field):
        """checks if column exist on database"""
        if field in df.columns:
            return True

        # return the original dataframe
        self.validated_df = df
        return False

    def add_errors(self, df, failed_indices):
        """this adds error column on validated dataframe"""
        if failed_indices == []:
            return df
        with_error = df

        if "error" not in df.columns:
            with_error["error"] = ""

        with_error.loc[failed_indices, "error"] = (
            with_error.loc[failed_indices, "error"]
            + f", {self.field} - {self.error_message}"
        )
        return with_error

class BaseDataFrameValidator(ABC):
    ''''this is a dataframe level validator used to validate asset dataframe'''
    def __init__(self,df,**kwargs) -> None:
        self.df = df
        self.validated_df = None
        self.failed_indices = []
        self.required_fields = []

    def append_to_failed_indices(self,indices):
        self.failed_indices += indices
        self.failed_indices = list(set(self.failed_indices))

    def run_column_validators(self):
        '''this handles running of multiple validators'''
        fields = [(x,getattr(self, x)) for x in dir(self)]

        #get all validator fields
        column_validators = [x for x in fields
                             if isinstance(x[1], Validator)]
        
        to_filter_df = self.df
        for field_name , validator in column_validators:
            column_validators = validator.validators
            
            for column_validator in column_validators:
                column_validator.validate(to_filter_df,field_name)
                to_filter_df = column_validator.validated_df
                
                self.append_to_failed_indices(column_validator.failed_indices)
            self.validated_df = column_validator.validated_df
        
    def validate(self):
        '''this is the entry point method to all class'''
        self.run_column_validators()
        

    def get_required_fields(self):
        return self.required_fields