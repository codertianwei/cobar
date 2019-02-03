package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.util.StringUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: shushenglin
 * Date:   2019/2/1 17:55
 */
public class PartitionByMaskAndMap extends PartitionByMask {
    public PartitionByMaskAndMap(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    public PartitionByMaskAndMap(String functionName) {
        super(functionName);
    }

    protected int indexOffset = 0;
    protected int partitionMask = 0;

    protected String fileMapPath;

    public void setPartitionMask(int partitionMask) {
        this.partitionMask = partitionMask;
    }

    public void setIndexOffset(int indexOffset) {
        this.indexOffset = indexOffset;
    }

    public void setFileMapPath(String fileMapPath) {
        this.fileMapPath = fileMapPath;
    }

    private Map<Integer, Integer> app2Partition;

    @Override
    protected int partitionIndex(long hash) {
        int idx = super.partitionIndex(hash);
        if (partitionMask > 0) {
            idx = idx & partitionMask;
        }
        if (app2Partition != null) {
            Integer node = app2Partition.get(idx);
            if (node != null) {
                idx = node;
            }
        }
        if (indexOffset > 0 && idx >= indexOffset) {
            idx -= indexOffset;
        }
        return idx;
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByMaskAndMap partitionFunc = new PartitionByMaskAndMap(functionName, args);
        partitionFunc.serverIdMask = getServerIdMask();
        partitionFunc.fileMapPath = fileMapPath;
        partitionFunc.indexOffset = indexOffset;
        partitionFunc.partitionMask = partitionMask;
        return partitionFunc;
    }

    @Override
    public void initialize() {
        initMap();
    }

    private void initMap() {
        if (StringUtil.isEmpty(fileMapPath)) {
            return;
        }
        app2Partition = new HashMap<Integer, Integer>();
        InputStream fin = null;
        try {
            fin = new FileInputStream(new File(fileMapPath));
            BufferedReader in = new BufferedReader(new InputStreamReader(fin));
            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//"))
                    continue;
                int ind = line.indexOf('=');
                if (ind < 0)
                    continue;
                try {
                    int key = Integer.parseInt(line.substring(0, ind).trim());
                    int pid = Integer.parseInt(line.substring(ind + 1).trim());
                    app2Partition.put(key, pid);
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                fin.close();
            } catch (Exception e2) {
            }
        }
    }

}
