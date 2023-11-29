namespace GO.Workerservice;

public class Process 
{

    private DatabaseService DatabaseService;
    private ScaleDimensionerResult ScaleDimensionerResult;

    public Process(DatabaseService databaseService, ScaleDimensionerResult scaleDimensionerResult) 
    {
        this.DatabaseService = databaseService;
        this.ScaleDimensionerResult = scaleDimensionerResult;
    }

    public async Task ProcessPackageAsync() 
    {

    }

}