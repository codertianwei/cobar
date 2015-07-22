package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Author: shusl
 * Date:   15/7/22 17:12
 */
public class PartitionByStringCRC32 extends PartitionByMod implements RuleAlgorithm {
	public PartitionByStringCRC32(String functionName){
		 this(functionName,null);
	}
	public PartitionByStringCRC32(String functionName, List<Expression> arguments) {
	        super(functionName, arguments);
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
		PartitionByStringCRC32 partitionFunc = new PartitionByStringCRC32(functionName, arguments);
		partitionFunc.count = count;
		return partitionFunc;
	}

	@Override
	public RuleAlgorithm constructMe(Object... objects) {
		List<Expression> args = new ArrayList<Expression>(objects.length);
		for (Object obj : objects) {
			args.add((Expression) obj);
		}
		PartitionByStringCRC32 partitionFunc = new PartitionByStringCRC32(functionName, args);
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
		if (arg == UNEVALUATABLE) {
			throw new IllegalArgumentException("argument is UNEVALUATABLE");
		}
		String key = String.valueOf(arg);
		CRC32 crc32 = new CRC32();
		crc32.update(key.getBytes());
		rst[0] = partitionIndex(crc32.getValue());
		return rst;
	}
}
