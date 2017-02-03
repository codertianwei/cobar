package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Author: shushenglin
 * Date:   2017/2/3 09:52
 */
public class PartitionBySubString extends FunctionExpression implements RuleAlgorithm {

	private Map<String, Integer> serverIdMap = new ConcurrentHashMap<String, Integer>();
	int serverIdLength = 6;
	boolean enableIdMap = true;

	public int getServerIdLength() {
		return serverIdLength;
	}

	public void setServerIdLength(int serverIdLength) {
		this.serverIdLength = serverIdLength;
	}

	public boolean isEnableIdMap() {
		return enableIdMap;
	}

	public void setEnableIdMap(boolean enableIdMap) {
		this.enableIdMap = enableIdMap;
	}

	public PartitionBySubString(String functionName, List<Expression> arguments) {
		super(functionName, arguments);
	}

	@Override
	public RuleAlgorithm constructMe(Object... objects) {
		List<Expression> args = new ArrayList<Expression>(objects.length);
		for (Object obj : objects) {
			args.add((Expression) obj);
		}
		PartitionBySubString rst = new PartitionBySubString(functionName, args);
		rst.serverIdLength = serverIdLength;
		rst.enableIdMap = enableIdMap;
		return rst;
	}

	@Override
	public void initialize() {
	}

	@Override
	public Integer[] calculate(Map<?, ?> parameters) {
		Object arg = arguments.get(0).evaluation(parameters);
		validateArgs(arg);
		Integer[] rst = new Integer[1];
		rst[0] = parseServerId(arg);
		return rst;
	}

	protected void validateArgs(Object arg) {
		if (arg == null) {
			throw new IllegalArgumentException("partition key is null ");
		} else if (arg == UNEVALUATABLE) {
			throw new IllegalArgumentException("argument is UNEVALUATABLE");
		}
	}

	protected Integer parseServerId(Object arg) {
		String uid = String.valueOf(arg);
		String sidStr = uid.substring(uid.length() - serverIdLength);
		Integer sid;
		if (enableIdMap) {
			sid = serverIdMap.get(sidStr);
			if (sid == null) {
				sid = Integer.valueOf(sidStr);
				serverIdMap.put(sidStr, sid);
			}
		}else {
			sid = Integer.valueOf(sidStr);
		}
		return sid;
	}

	@Override
	public FunctionExpression constructFunction(List<Expression> arguments) {
		if (arguments == null || arguments.size() != 1)
			throw new IllegalArgumentException("function " + getFunctionName() + " must have 1 arguments but is "
					+ arguments);
		Object[] args = new Object[arguments.size()];
		int i = -1;
		for (Expression arg : arguments) {
			args[++i] = arg;
		}
		return (FunctionExpression) constructMe(args);
	}
}
