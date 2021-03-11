package com.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import static ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision;


public class GeohashUDF extends GenericUDF {

    private DoubleObjectInspector doubleObjectInspector01;
    private DoubleObjectInspector doubleObjectInspector02;
    private IntObjectInspector intObjectInspector;

    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentLengthException("Class takes only 3 arguments: Double, Double, Int");
        }
        // 1. Check if the correct parameter type is received
        ObjectInspector a = objectInspectors[0];
        ObjectInspector b = objectInspectors[1];
        ObjectInspector c = objectInspectors[2];

        if (!(a instanceof DoubleObjectInspector) || !(b instanceof DoubleObjectInspector)
                || !(c instanceof IntObjectInspector)) {
            throw new UDFArgumentException("first and second arguments must be a double, third argument must be a int");
        }

        this.doubleObjectInspector01 = (DoubleObjectInspector) a;
        this.doubleObjectInspector02 = (DoubleObjectInspector) b;
        this.intObjectInspector = (IntObjectInspector) c;

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        Double lat = this.doubleObjectInspector01.get(deferredObjects[0].get());
        Double lng = this.doubleObjectInspector02.get(deferredObjects[1].get());
        Integer len = this.intObjectInspector.get(deferredObjects[2].get());

        return geoHashStringWithCharacterPrecision(lat, lng, len);
    }

    public String getDisplayString(String[] strings) {
        if (strings.length == 3) {
            return "geo_hash(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ")";
        } else {
            return "The parameters passed in are incorrect...";
        }
    }
}
