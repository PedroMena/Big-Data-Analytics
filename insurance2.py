from mrjob.job import MRJob                                                                                                                                                                      
import statistics
class avg(MRJob):                                                                                                                                                                           
                                                                                                                                                                                            
    def mapper(self,key,value):                                                                                                                                                             
                                                                                                                                                                                            
        (age,gender,bmi,children,smoker,region,charges)=value.split(',')                                                                                                                    
        a= [float(charges),float(bmi),int(children)]                                                                                                                                                                  
        yield age, a                                                                                                                                                       

                                                                                                                                                                                            
    def reducer(self,age,charges):                                                                                                                                                          
                                                                                                                                                                                            
        totalchar=0
        totalbmi=0
        totalchild=0                                                                                                                                                                             
        chargs=[]
        bmis=[]
        childs=[]                                                                                                                                                                                    
        num=0                                                                                                                                                                               
                                                                                                                                                                                            
        for i in charges:
            num=num+1  
            totalchar=totalchar+i[0]
            totalbmi=totalbmi+i[1]
            totalchild=totalchild+i[2]
            chargs.append(i[0])
            bmis.append(i[1])
            childs.append(i[2])
                                                                                                                                                                                            
        yield age,[totalchar/num,statistics.stdev(chargs),totalbmi/num,statistics.stdev(bmis),totalchild/num,statistics.stdev(childs)]                                                                                                                                                                 
                                                                                                                                                                                            
                                                                                                                                                                                            
avg.run()           