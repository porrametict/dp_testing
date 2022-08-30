from pydantic import BaseSettings

class Settings(BaseSettings):
    ckan_sysadmin_api_key : str 
    ckan_site_url : str
    
    @property
    def local_file_callback_url (self) -> str :
        return f"{self.ckan_site_url}/api/3/action/dataconnect_lp_tockan"
    
    @property
    def ckan_resource_show_url (self) -> str :
        return f"{self.ckan_site_url}/api/3/action/resource_show"

    
    @property 
    def dataconnect_store_log_url (self) -> str :
        return f"{self.ckan_site_url}/api/3/action/dataconnect_store_log"

    @property 
    def task_status_show_url (self) -> str :
        return f"{self.ckan_site_url}/api/3/action/task_status_show"

    @property 
    def task_status_update_url (self) -> str :
        return f"{self.ckan_site_url}/api/3/action/task_status_update"

        
    # class Config :
    #     env_file = ".env"