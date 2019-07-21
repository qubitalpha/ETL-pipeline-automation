from data_models.data_model import DataModel

class DataModelApiIcelandConcerts(DataModel):
    @property
    def create_statement(self):
        return """
            create table iceland.concerts (
                event            varchar(1024)  default null
              , band             varchar(255)   not null
              , hall             varchar(1024)  not null
              , timestamp        timestamp      not null
            );
        """
    
    @property
    def drop_statement(self):
        return """
            drop table if exists iceland.concerts
        """
