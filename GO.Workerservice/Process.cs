using System.Data.Odbc;

namespace GO.Workerservice;

public class Process 
{

    private DatabaseService DatabaseService;
    private ScaleDimensionerResult ScaleDimensionerResult;

    private Configuration Configuration;

    public Process(Configuration configuration, DatabaseService databaseService, ScaleDimensionerResult scaleDimensionerResult) 
    {
        this.Configuration = configuration;
        this.DatabaseService = databaseService;
        this.ScaleDimensionerResult = scaleDimensionerResult;
    }

    public async Task ProcessPackageAsync(ScaleDimensionerResult scaleDimensionerResult) 
    {
        if (this.ScaleDimensionerResult == null || this.DatabaseService == null) return;

        try 
        {
            string freightLetterNumber = this.ScaleDimensionerResult.Barcode;
            PackageData packageData = await DatabaseService.GetOrderAsync(freightLetterNumber); // 1

            if (packageData == null) return;

            ScanData scanData = await DatabaseService.GetScanAsync(freightLetterNumber); // 2

            string scanLocation = packageData.df_ndl;
            string date = packageData.df_datauftannahme;
            string orderNumber = packageData.df_lfdnrauftrag;

            if (scanData == null)
            {
                await DatabaseService.AddScanAsync(scaleDimensionerResult, packageData); // 3

                int? realWeight = await DatabaseService.GetWeightAsync(); // 4

                if (realWeight == null) return;

                await DatabaseService.UpdateWeightAsync((int) realWeight, scanLocation, date, orderNumber); // 5

                bool? packageExists = await DatabaseService.DoesPackageExistAsync(scanLocation, date, orderNumber); // 6

                if (packageExists == null) return;

                float volumeFactor;

                if (packageData.df_ndl == this.Configuration.ScanLocation // 7
                    && this.Configuration.ExceptionList!.ContainsKey(packageData.df_kundennr)) 
                {
                    volumeFactor = this.Configuration.ExceptionList[packageData.df_kundennr];
                } else {
                    volumeFactor = this.Configuration.DefaultVolumeFactor;
                }

                await this.DatabaseService.CreatePackageAsync(packageData, scaleDimensionerResult, volumeFactor); // 8

                int? volumeWeight = await this.DatabaseService.GetTotalWeightAsync(packageData.df_ndl,   // 9
                                                                                  packageData.df_datauftannahme, 
                                                                                  packageData.df_lfdnrauftrag);

                if (volumeWeight == null) return;

                await this.DatabaseService.UpdateTotalWeightAsync((int) volumeWeight, scanLocation, date, orderNumber); // 10

                int weight = (int) realWeight;

                if (packageData.df_ndl == this.Configuration.ScanLocation && volumeWeight > realWeight) {
                    weight = (int) volumeWeight;
                } 

                await this.DatabaseService.UpdateOrderWeightAsync((int) realWeight, scanLocation, date, orderNumber); // 11
            } 
            else
            {

            }
        }
        catch (OdbcException e) 
        {
            Console.WriteLine(e.StackTrace);
        }

    }

}