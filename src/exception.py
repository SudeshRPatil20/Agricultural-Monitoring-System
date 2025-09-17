# error handling in exeption
import sys
from src.logger import logging

#sys provides exeption handling like any exeption occur sys will control that exeption

def error_message_details(error,error_detail:sys):
    _,_,exc_tb=error_detail.exc_info()
    file_name=exc_tb.tb_frame.f_code.co_filename
    error_message="the error occur in python script [{0}] line number [{1}] error message [{2}]".format(
        file_name,exc_tb.tb_lineno,str(error) #with the help of this we can trace the error occur in file
    )
    
    return error_message
    
class CustomException(Exception):
    def __init__(self,error_message,error_detail:sys):
        super().__init__(error_message)
        self.error_message=error_message_details(error_message, error_detail=error_detail)
        
    def __str__(self):
        return self.error_message
        
