using System.Data.Odbc;

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

    public async Task ProcessPackageAsync(ScaleDimensionerResult scaleDimensionerResult) 
    {
        if (this.ScaleDimensionerResult == null || this.DatabaseService == null) return;

        try 
        {
            string freightLetterNumber = this.ScaleDimensionerResult.Barcode;
            PackageData packageData = await DatabaseService.GetOrderAsync(freightLetterNumber);

            if (packageData == null) return;

            ScanData scanData = await DatabaseService.GetScanAsync(freightLetterNumber);

            string scanLocation;
            string date;
            string orderNumber;

            if (scanData == null)
            {
                await DatabaseService.AddScanAsync(scaleDimensionerResult, packageData);

                int? weight = await DatabaseService.GetWeightAsync();

                await DatabaseService.UpdateWeightAsync(weight, scanLocation, date, orderNumber);

            } 
            else
            {

            }
        }
        catch (OdbcException e) 
        {

        }

    }

}