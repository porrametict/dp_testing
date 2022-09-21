from csv import writer

def write_result(result):

    with open('test_json_o_results/v2_json_complete.csv', 'a', newline='') as f_object:  
        writer_object = writer(f_object)
        writer_object.writerow(result)  
        f_object.close()
