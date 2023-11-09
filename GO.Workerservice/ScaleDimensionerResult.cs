namespace GO.Workerservice;
public enum Shape 
{
    UnknownShaoe = 0x00,
    Cylinder = 0x01,
    Cubic = 0x04,
    Tyre = 0x07
}

public enum ScaleState 
{
    NotOk = -1,
    Ok = 0,
    Unstable = 1,
    MultipleItes = 3,
    Underloaded = 4,
    Overloaded = 5,
}

public enum DimensionerState 
{
    LFT = 0,
    NoLFT = 1
}

/*
    Byte        Name
    1           Gewichtsflag
    2 - 7       Gewicht
    8           Volumenflag
    9 - 20      Volumen
    21          Bar code flag
    42 - 81     Barcode
    82 - 85     Statuscode Dimensionierer
    86          Status Waage
    87 - 88     Known shape
    89          Waage LFT
    90          Volumen LFT
    91          CR
*/
public sealed class ScaleDimensionerResult 
{

    public UInt16 Weight;
    public UInt16 Length;
    public UInt16 Width;
    public UInt16 Height;

    public string? Barcode;

    public ScaleState ScaleState;
    public DimensionerState DimensionerState;

    public Shape Shape;

    public bool ScaleLFT;
    public bool DimensionerLFT;

    public ScaleDimensionerResult(string data)
    {
        const Char PLACEHOLDER = '#';
        const Char SPACE = ' ';
        const Char ZERO = '0';
        const Char WEIGHTFLAG = 'W';
        const Char VOLUMEFLAG = 'V';
        const Char CARRIAGERETURN = '\r';
        const string ERROR = "ERROR";

        Char weightFlag;
        UInt16 weight;
        Char volumeFlag;
        UInt16 length;
        UInt16 width;
        UInt16 height;
        Char barcodeFlag;
        string? barcode;
        DimensionerState dimensionerState;
        ScaleState scaleState;
        Shape shape;
        bool scaleLFT;
        bool dimensionerLFT;
        Char CR;

        weightFlag = (Char) data[0];
        weight = UInt16.Parse(data[1..6]);
        volumeFlag = (Char) data[7];
        length = UInt16.Parse(data[8..11]);
        width = UInt16.Parse(data[12..15]);
        height = UInt16.Parse(data[16..19]);
        barcodeFlag = (Char) data[20];
        barcode = data[41..80];
        dimensionerState = (DimensionerState) UInt16.Parse(data[81..84]);
        scaleState = (ScaleState) Char.GetNumericValue(data[85]) - ZERO;
        shape = (Shape) UInt16.Parse(data[86..87]);
        scaleLFT = data[88] == 1;
        dimensionerLFT = data[89] == 1;
        CR = (Char) data[90];

        barcode = barcode.Remove(PLACEHOLDER);
        barcode = barcode.Remove(SPACE);
        
        if (barcode.Equals(ERROR)) {
            barcode = null;
        }

        if (!weightFlag.Equals(WEIGHTFLAG)) {
            // ToDo: error
        }

        if (!volumeFlag.Equals(VOLUMEFLAG)) {
            // ToDo: error
        }

        if (!CR.Equals(CARRIAGERETURN)) {
            // ToDo: error
        }

        this.Weight = weight;
        this.Length = length;
        this.Width = width;
        this.Height = height;
        this.Barcode = barcode;
        this.Shape = shape;
        this.DimensionerState = dimensionerState;
        this.ScaleState = scaleState;
        this.ScaleLFT = scaleLFT;
        this.DimensionerLFT = dimensionerLFT;
    }

    public int GetVolume() 
    {
        return this.Length * this.Width * this.Height;
    }
}