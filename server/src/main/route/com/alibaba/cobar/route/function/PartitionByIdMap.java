package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import com.linkedin.paldb.api.Configuration;
import com.linkedin.paldb.api.PalDB;
import com.linkedin.paldb.api.StoreReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: shushenglin
 * Date:   2017/2/2 08:30
 */
public class PartitionByIdMap extends PartitionBySubString {

	private String uidMapFile;
	private String expectSize = "1000";
	private StoreReader reader;

	public PartitionByIdMap(String functionName){
		this(functionName, null);
	}

	public PartitionByIdMap(String functionName, List<Expression> arguments) {
		super(functionName, arguments);
	}

	public void setUidMapFile(String uidMapFile) {
		this.uidMapFile = uidMapFile;
	}

	public void setExpectSize(String expectSize) {
		this.expectSize = expectSize;
	}

	@Override
	public Object evaluationInternal(Map<?, ?> parameters) {
		return calculate(parameters)[0];
	}

	@Override
	public void init() {
		initialize();
	}

	@Override
	public RuleAlgorithm constructMe(Object... objects) {
		List<Expression> args = new ArrayList<Expression>(objects.length);
		for (Object obj : objects) {
			args.add((Expression) obj);
		}
		PartitionByIdMap rst = new PartitionByIdMap(functionName, args);
		rst.uidMapFile = uidMapFile;
		rst.expectSize = expectSize;
		rst.serverIdLength = serverIdLength;
		rst.enableIdMap = enableIdMap;
		return rst;
	}

	@Override
	public void initialize() {
		Configuration configuration = new Configuration();
		configuration.set(Configuration.CACHE_INITIAL_CAPACITY, expectSize);
		reader = PalDB.createReader(new File(uidMapFile), configuration);
	}

	@Override
	public Integer[] calculate(Map<?, ?> parameters) {
		Integer[] rst = new Integer[1];
		Object arg = arguments.get(0).evaluation(parameters);
		validateArgs(arg);
		Integer pid = reader.getInt(arg, -1);
		if (pid == -1) {
			rst[0] = parseServerId(arg);
		} else {
			rst[0] = pid;
		}
		return rst;
	}
}
