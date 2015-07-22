package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:dragon829@gmail.com">lostdragon</a>
 */
public class PartitionByMod extends FunctionExpression implements RuleAlgorithm {
    public PartitionByMod(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }
    public PartitionByMod(String functionName){
        this(functionName,null);
    }

    protected int count;

    public void setPartitionCount(String partitionCount) {
        this.count = Integer.parseInt(partitionCount);
    }

    protected int partitionIndex(long hash) {
        return (int)(hash % count);
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters)[0];
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 1)
            throw new IllegalArgumentException("function "
                    + getFunctionName()
                    + " must have 1 argument but is "
                    + arguments);
        PartitionByMod partitionFunc = new PartitionByMod(functionName, arguments);
        partitionFunc.count = count;
        return partitionFunc;
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByMod partitionFunc = new PartitionByMod(functionName, args);
        partitionFunc.count = count;
        return partitionFunc;
    }

    @Override
    public void initialize() {
        init();
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
        rst[0] = partitionIndex(key.longValue());
        return rst;
    }
}
