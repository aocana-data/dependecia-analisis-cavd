
class InterfaceGetters:
    def get_data(self):
        return {
            "dataframe"     :   self.database,
            "json_config"   :   self.config ,
        }
    
    def get_config(self):
        return self.config
    
    def get_cnx(self):
        return self.cnx
    
    def get_table(self):
        return self.table
    
    
    def get_config_path(self):
        
        if not getattr(self, "config_path",False):
            return False
        
        return self.config_path
    
    def builder_get_database(self):
        return self.database
    
    def get_base_dir(self):
        return self.BASE_DIR