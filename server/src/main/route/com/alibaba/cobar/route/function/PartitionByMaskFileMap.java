package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.util.StringUtil;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author codertianwei
 * @author <a href="mailto:tianwei.luo@foxmail.com">codertianwei</a>
 */
public class PartitionByMaskFileMap extends PartitionByFileMap {
    private static final Logger LOGGER = Logger.getLogger(PartitionByMaskFileMap.class);

    protected int serverIdMask;

    public PartitionByMaskFileMap(String functionName) {
        super(functionName, null);
    }

    public PartitionByMaskFileMap(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    public void setPartitionCount(String partitionCount) {
        this.serverIdMask = Integer.parseInt(partitionCount);
    }

    @Override
    public Integer[] calculate(Map<? extends Object, ? extends Object> parameters) {
        Integer[] rst = new Integer[1];
        Object arg = arguments.get(0).evaluation(parameters);
        if (arg == null) {
            throw new IllegalArgumentException("partition key is null ");
        } else if (arg == UNEVALUATABLE) {
            throw new IllegalArgumentException("argument is UNEVALUATABLE");
        }
        Number key;
        if (arg instanceof Number) {
            key = (Number) arg;
        } else if (arg instanceof String) {
            key = Long.parseLong((String) arg);
        } else {
            throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
        }
        Integer pid = app2Partition.get(String.valueOf(partitionIndex(key.longValue())));
        if (pid == null) {
            rst[0] = defaultNode;
        } else {
            rst[0] = pid;
        }
        return rst;
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByMaskFileMap rst = new PartitionByMaskFileMap(functionName, args);
        rst.fileMapPath = fileMapPath;
        rst.defaultNode = defaultNode;
        rst.serverIdMask = serverIdMask;
        return rst;
    }

    protected int partitionIndex(long hash) {
        return (int)(hash & serverIdMask);
    }
}
