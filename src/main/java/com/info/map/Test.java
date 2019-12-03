package com.info.map;


import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) {
        List<String> list = Arrays.asList("a,b,c", "1,2,3");
       /* System.out.println(list.stream()
                .map(x -> x.split(","))
                .flatMap(Arrays::stream)
                .collect(Collectors.toList()));*/
        Stream<String[]> stream = list.stream().map(new Function<String, String[]>() {
            @Override
            public String[] apply(String s) {
                String[] split = s.split(",");
                for (String s1:split) {
                    System.out.print(s1+" ");
                }
                return split;
            }
        });
        List<String[]> collect = stream.collect(Collectors.toList());

    }

}
