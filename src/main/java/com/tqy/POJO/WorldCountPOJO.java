package com.tqy.POJO;

public class WorldCountPOJO {
    String word;
    Integer count;

    public WorldCountPOJO(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WorldCountPOJO{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
