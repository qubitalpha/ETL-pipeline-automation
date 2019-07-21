from data_models.data_model import DataModel

class DataModelApiIcelandDrivers(DataModel):
    @property
    def create_statement(self):
        return """
            create table iceland.drivers (
                from_column            varchar(1024)  not null
              , to_column             varchar(1024)  default null
              , timestamp        timestamp      not null
            );
        """
    
    @property
    def drop_statement(self):
        return """
            drop table if exists iceland.drivers
        """
