from mrjob.job import MRJob                                                                                                                                                                 
                                                                                                                                                                                            
                                                                                                                                                                                            
class avg(MRJob):                                                                                                                                                                           
                                                                                                                                                                                            
    def mapper(self,key,value):                                                                                                                                                             
                                                                                                                                                                                            
        (user,movie,rating,timestamp)=value.split('	')                                                                                                                                                                                                                                                                                               
        yield rating, 1                                                                                                                                                
                                                                                                                                                                                            
    def reducer(self,rating,ones):                                                                                                                                                          
                                                                                                                                                                                            
        total=0                                                                                                                                                                                 
                                                                                                                                                                                            
        for i in ones:                                                                                                                                                                                       
            total+=1                                                                                                                                                                        
                                                                                                                                                                                              
                                                                                                                                                                                            
        yield rating,total                                                                                                                                                                 
                                                                                                                                                                                            
                                                                                                                                                                                            
                                                                                                                                                                                            
avg.run() 