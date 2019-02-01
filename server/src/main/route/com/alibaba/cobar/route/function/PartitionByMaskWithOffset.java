package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;

import java.util.ArrayList;
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
     * partition id offset
     */
    protected int indexOffset = 0;
    /**
	 * 超过该值的uid，才通过mask方式计算，否则返回定义默认分区
	 */
	protected long uidOffset = 0;
	
	protected int defaultPartitionId = 0;

	public void setUidOffset(long uidOffset) {
		this.uidOffset = uidOffset;
	}

	public void setDefaultPartitionId(int defaultPartitionId) {
		this.defaultPartitionId = defaultPartitionId;
	}

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByMaskWithOffset partitionFunc = new PartitionByMaskWithOffset(functionName, args);
        partitionFunc.serverIdMask = serverIdMask;
        partitionFunc.defaultPartitionId = defaultPartitionId;
        partitionFunc.indexOffset = indexOffset;
        partitionFunc.uidOffset = uidOffset;
        return partitionFunc;
    }

    @Override
	protected int partitionIndex(long hash) {
        int serverId = getServerId(hash);
        if (indexOffset > 0 && serverId >= indexOffset) {
            serverId -= indexOffset;
        }
        return serverId;
	}

    private int getServerId(long hash) {
        if (hash < uidOffset) {
			return defaultPartitionId;
		}
        return super.partitionIndex(hash);
    }

    public int getIndexOffset() {
        return indexOffset;
    }

    public void setIndexOffset(int indexOffset) {
        this.indexOffset = indexOffset;
    }
}
