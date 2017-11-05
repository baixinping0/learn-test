package com.bxp.hadooptest.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserWritable implements WritableComparable <UserWritable>{
    private int id;
    private String name;
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public UserWritable() {
    }
    public UserWritable(int id, String name) {
        set(id, name);
    }
    public void set(int id, String name) {
        this.setId(id);
        this.setName(name);
    }


    //注意！！！：此两个方法读写字段的顺序必须保持一致，不一致将会出错
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
    }


    public int compareTo(UserWritable o) {
        int comp = Integer.valueOf(this.getId()).compareTo(o.getId());
        if(0 != comp)
            return comp;
        return this.getName().compareTo(o.getName());
    }

    @Override
    public String toString() {
        return id +"\t"+ name;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserWritable that = (UserWritable) o;

        if (id != that.id) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

}
