package com.alibaba.cobar.route.function;

import com.alibaba.cobar.parser.ast.expression.Expression;

import java.util.List;

/**
 * Author: shushenglin
 * Date:   2018/9/11 13:51
 */
public class PartitionByMaskWithOffset extends PartitionByMask {
	public PartitionByMaskWithOffset(String functionName, List<Expression> arguments) {
		super(functionName, arguments);
	}

	public PartitionByMaskWithOffset(String functionName) {
		super(functionName);
	}

	/**
	 * 超过该值的uid，才通过mask方式计算，否则返回定义默认分区
	 */
	protected long uidOffset = 0;
	
	protected int defaultPartitionId = 1;

	public void setUidOffset(long uidOffset) {
		this.uidOffset = uidOffset;
	}

	public void setDefaultPartitionId(int defaultPartitionId) {
		this.defaultPartitionId = defaultPartitionId;
	}

	@Override
	protected int partitionIndex(long hash) {
		if (hash < uidOffset) {
			return defaultPartitionId;
		}
		return super.partitionIndex(hash);
	}
}
