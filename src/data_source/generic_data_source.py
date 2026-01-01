from abc import ABC, abstractmethod

class GenericDataSource(ABC):
    """Classe genérica para coleta de Dados."""

    def __init__(self):
        pass
    
    @abstractmethod
    def start(self):
        return NotImplementedError('Método ainda não implementado.')
    
    @abstractmethod
    def get_data(self):
        return NotImplementedError('Método ainda não implementado.')
    
    @abstractmethod
    def transform_data(self):
        return NotImplementedError('Método ainda não implementado.')
    
    @abstractmethod
    def validate_data(self):
        return NotImplementedError('Método ainda não implementado.')
    
    @abstractmethod
    def load_data(self):
        return NotImplementedError('Método ainda não implementado.')