## installation

###  download
```
git clone https://git.igridproject.info/data4thai/fastapi-data-prepare.git
cd fastapi-data-prepare
```
### config .env file
```
cp .env-template .env
nano .env
```
- `CKAN_SYSADMIN_API_KEY` is the system admin' API key  you can find it on the profile page
- `CKAN_SITE_URL` is the URL of your CKAN site . You can put `http://"host.docker.internal` if CKAN is installed on the same machine.
- `VERSION` is the dataconnect version, by default, it is `v1`


### run with docker-compose
```
docker-compose up --build -d
```

### run with docker command
```
docker build -t ckan_dataconnect_dp .
docker run -d -p 8000:8000 --add-host host.docker.internal:host-gateway -e CKAN_SYSADMIN_API_KEY="{{admin_token}}" -e CKAN_SITE_URL="{{ckan-site-url}}" ckan_dataconnect_dp 
```


##  Fetch Prepare
**url**  : `/prepare-data`
**method** : `POST`

**body** 
```
{
        "resource_id" : "test", // resource id
        "url" : url, // address 
        "content_type" : "CSV", // content_type  MUST BE ["JSON","CSV","XLS","XLSX"]
        "param" : {} // param   || .data => path_filter , .request_method == ["GET","POST"], .http_headers
        "schemas" : schemas,  // schemas
        "data" : data , // ข้อมูล  กรณี JSON ต้องการ กรณีอื่นๆ ไม่ต้องการก็ได้
}
```

**Response** 

```
{
        "result" : ['list of dict or empty list']
}

```

### Example
**Body** 
```
{
        "resource_id" : "87adaf67-3b78-434a-85ad-f51a47fdd3a6",
        "url" : "http://example.com",
        "content_type" : "JSON",
        "param" : {
                "data" : ".",
                "http_header" : {"Authentication" : "Bareer 2kdujwelfw"}
        }
        "data" : [
                {
                        "col_1":"val_1",
                },{
                        "col_2" : "val_2"
                }
        ],
        "schemas" : [
                {

                "field" : "col_1",
                "new_field_name" :"new_col",
                "selected" : true
                },
                {

                "field" : "col_2",
                "new_field_name" :"new_col2",
                "selected" : false
                }
        ]
}
```
**Response**
```
{
        "result" : [
                {"new_col":"val_1"}
        ]
}
```

