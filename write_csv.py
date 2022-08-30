from csv import writer

def write_result(result):

    with open('json_test_results/json_o_test_results.csv', 'a', newline='') as f_object:  
        writer_object = writer(f_object)
        writer_object.writerow(result)  
        f_object.close()
